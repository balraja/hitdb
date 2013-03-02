/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package org.hit.facade;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.hit.communicator.Communicator;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.PartitioningType;
import org.hit.db.model.Schema;
import org.hit.db.model.mutations.SingleKeyMutation;
import org.hit.di.HitFacadeModule;
import org.hit.distribution.DistributedPartitioner;
import org.hit.distribution.KeyPartitioner;
import org.hit.distribution.KeySpace;
import org.hit.distribution.LinearPartitioner;
import org.hit.event.CreateTableMessage;
import org.hit.event.CreateTableResponseMessage;
import org.hit.event.DBOperationFailureMessage;
import org.hit.event.DBOperationSuccessMessage;
import org.hit.event.PerformDBOperationMessage;
import org.hit.registry.RegistryService;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;
import org.hit.util.Pair;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Defines the contract for a class that acts as a client to the database.
 * Primarily this class reads the commands/statements from the command line
 * and after parsing the same sends it down to the servers.
 *
 * @author Balraja Subbiah
 */
public class HitDBFacade
{
    /**
     * A simple class that wraps the functionality of handling the
     * response from the server.
     */
    private class CommunicatorResponseHandlerTask implements Runnable
    {
        private final Message myMessage;

        /**
         * CTOR
         */
        public CommunicatorResponseHandlerTask(Message message)
        {
            super();
            myMessage = message;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            if (myMessage instanceof CreateTableResponseMessage) {
                CreateTableResponseMessage createTableResponse =
                    (CreateTableResponseMessage) myMessage;

                SettableFuture<NodeID> tableCreationFuture =
                    myPhasedTableCreationFutureMap.get(
                        createTableResponse.getTableName());

                if (createTableResponse.isIsSuccessful()) {
                    tableCreationFuture.set(createTableResponse.getNodeId());
                }
                else {
                    tableCreationFuture.setException(
                        new RuntimeException(
                            String.format(TABLE_CREATION_FAILURE,
                                          createTableResponse.getTableName(),
                                          createTableResponse.getNodeId())));

                }
            }
            else if (myMessage instanceof DBOperationSuccessMessage) {
                DBOperationSuccessMessage dbOperationSuccessMessage =
                    (DBOperationSuccessMessage) myMessage;
                DBOperation appliedOperation =
                    dbOperationSuccessMessage.getAppliedDBOperation();

                if (appliedOperation instanceof WrappedMutation) {

                    long id = ((WrappedMutation) appliedOperation).getID();
                    SettableFuture<DBOperationResponse> future  =
                        myOperationIDToFutureMap.get(Long.valueOf(id));

                    future.set(new DBOperationResponse(
                        appliedOperation,
                        dbOperationSuccessMessage.getResult()));
                }
            }
            else if (myMessage instanceof DBOperationFailureMessage) {
                DBOperationFailureMessage dbOperationFailure =
                    (DBOperationFailureMessage) myMessage;

                DBOperation appliedOperation =
                   dbOperationFailure.getDBOperation();

                if (appliedOperation instanceof WrappedMutation) {

                    long id = ((WrappedMutation) appliedOperation).getID();
                    SettableFuture<DBOperationResponse> future  =
                        myOperationIDToFutureMap.get(Long.valueOf(id));
                    future.setException(
                        dbOperationFailure.getException());
                }
            }
        }
    }

    /**
     * A helper class that wraps the task of sending mutation to the
     * server.
     */
    private class SubmitDBOperationTask implements Runnable
    {
        private final NodeID myNodeID;

        private final DBOperation myOperation;

        private final long myOperationID;

        private final SettableFuture<DBOperationResponse> mySettableFuture;

        /**
         * CTOR
         */
        public SubmitDBOperationTask(NodeID                              nodeId,
                                     DBOperation                         operation,
                                     SettableFuture<DBOperationResponse> future,
                                     long operationID)
        {
            myNodeID = nodeId;
            myOperation = operation;
            mySettableFuture = future;
            myOperationID = operationID;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Processing request for performing the db operation " +
                     myOperation);

            myOperationIDToFutureMap.put(myOperationID, mySettableFuture);

            myCommunicator.sendTo(myNodeID,
                                  new PerformDBOperationMessage(
                                      myClientID,
                                      myOperation));
        }
    }

    /**
     * A helper class that wraps the task of sending mutation to the
     * server.
     */
    private class SubmitTableCreationTask implements Runnable
    {
        private final SettableFuture<TableCreationResponse> myFuture;

        private final Schema mySchema;

        /**
         * CTOR
         */
        public SubmitTableCreationTask(
            SettableFuture<TableCreationResponse> future,
            Schema schema)
        {
            super();
            myFuture = future;
            mySchema = schema;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Processing request for creating  table with schema " +
                     mySchema);

            List<NodeID> serverNodes = myRegistryService.getServerNodes();
            TableCreationResponseHandler tableCreationHandler =
                new TableCreationResponseHandler(
                    mySchema, myFuture, serverNodes);
            SettableFuture<NodeID> tableCreationFuture = SettableFuture.create();
            Futures.addCallback(tableCreationFuture, tableCreationHandler);
            myPhasedTableCreationFutureMap.put(mySchema.getTableName(),
                                               tableCreationFuture);

            CreateTableMessage message =
                new CreateTableMessage(myClientID, mySchema);
            NodeID serverNode = serverNodes.get(0);
            myCommunicator.sendTo(serverNode, message);
        }
    }

    private class TableCreationResponseHandler implements FutureCallback<NodeID>
    {
        private final SettableFuture<TableCreationResponse> myClientFuture;

        private final Set<NodeID> myCompletedNodes;

        private final List<NodeID> myServerNodes;

        private final Schema myTableSchema;

        /**
         * CTOR
         */
        public TableCreationResponseHandler(
            Schema                                tableSchema,
            SettableFuture<TableCreationResponse> clientFuture,
            List<NodeID>                          serverNodes)
        {
            myTableSchema = tableSchema;
            myClientFuture = clientFuture;
            myServerNodes = serverNodes;
            myCompletedNodes = new HashSet<>();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable t)
        {
            myClientFuture.setException(t);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(NodeID result)
        {
            myCompletedNodes.add(result);
            myServerNodes.remove(result);

            if (myServerNodes.isEmpty()) {
                myClientFuture.set(
                    new TableCreationResponse(myCompletedNodes, myTableSchema));
            }
            else {
                SettableFuture<NodeID> tableCreationFuture
                    = SettableFuture.create();
                Futures.addCallback(tableCreationFuture, this);
                CreateTableMessage message =
                    new CreateTableMessage(myClientID, myTableSchema);
                NodeID serverNode = myServerNodes.get(0);
                myCommunicator.sendTo(serverNode, message);
                myPhasedTableCreationFutureMap.put(
                    myTableSchema.getTableName(), tableCreationFuture);
            }
        }
    }

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HitDBFacade.class);

    private static final String TABLE_CREATION_FAILURE =
        "Creation of table %s on host %s failed";

    private final NodeID myClientID;

    private final Communicator myCommunicator;

    private final ListeningExecutorService myExecutorService;

    private final Map<Long, SettableFuture<DBOperationResponse>>
        myOperationIDToFutureMap;

    private final AtomicLong myOperationsCount;

    private final Map<String, SettableFuture<NodeID>>
        myPhasedTableCreationFutureMap;

    private final RegistryService myRegistryService;

    private final Map<String, KeyPartitioner<? extends Comparable<?>>>
        myTable2Partitoner;

    /**
     * CTOR
     */
    public HitDBFacade()
    {
        Injector injector = Guice.createInjector(new HitFacadeModule());
        myCommunicator = injector.getInstance(Communicator.class);
        myRegistryService = injector.getInstance(RegistryService.class);
        myClientID = injector.getInstance(NodeID.class);
        myTable2Partitoner = new HashMap<>();
        myOperationIDToFutureMap = new HashMap<>();
        myOperationsCount = new AtomicLong(0L);
        myPhasedTableCreationFutureMap = new HashMap<>();

        myExecutorService =
            MoreExecutors.listeningDecorator(
                Executors.newSingleThreadExecutor(
                    new NamedThreadFactory(HitDBFacade.class)));

    }

    /**
     * Applies the mutation to the database.
     */
    public <K extends Comparable<K>>
        ListenableFuture<DBOperationResponse> apply(
            SingleKeyMutation<K> mutation, String tableName)
    {
        @SuppressWarnings("unchecked")
        KeyPartitioner<K> partitioner =
            (KeyPartitioner<K>) myTable2Partitoner.get(tableName);

        if (partitioner == null) {
            Pair<PartitioningType, KeySpace<?>> keyMeta =
                myRegistryService.getTableKeyMetaData(tableName);

            @SuppressWarnings("unchecked")
            KeySpace<K> hashRing = (KeySpace<K>) keyMeta.getSecond();

            partitioner =
                keyMeta.getFirst() == PartitioningType.HASHABLE ?
                    new DistributedPartitioner<K>(hashRing)
                    : new LinearPartitioner<K>(hashRing);

            partitioner.distribute(myRegistryService.getServerNodes() );
            myTable2Partitoner.put(tableName, partitioner);
        }

        NodeID serverNode = partitioner.getNode(mutation.getKey());

        SettableFuture<DBOperationResponse> futureResponse =
            SettableFuture.create();
        long id = myOperationsCount.getAndIncrement();
        WrappedMutation wrappedMutation = new WrappedMutation(id, mutation);
        myExecutorService.submit(new SubmitDBOperationTask(
            serverNode, wrappedMutation, futureResponse, id));

        return futureResponse;
    }

    /**
     * Creates a new table in the database with the given <code>Schema</code>
     */
    public ListenableFuture<TableCreationResponse> createTable(Schema schema)
    {
        SettableFuture<TableCreationResponse> clientFuture =
            SettableFuture.create();

        myExecutorService.submit(
            new SubmitTableCreationTask(clientFuture, schema));

        return clientFuture;
    }

    /**
     * {@inheritDoc}
     */
    public void start()
    {
        while(!myRegistryService.isUp()) {
            // Wait till we connect to registry/zookeeper.
        }

        LOG.info("Connection to the registry " +
                 "service successfully established");

        myCommunicator.start();
        LOG.info("Messaging service got started successfully");

        myCommunicator.addMessageHandler(
             new MessageHandler() {
                 @Override
                 public void handle(Message message)
                 {
                      myExecutorService.execute(
                          new CommunicatorResponseHandlerTask(message));
                 }
             }
        );

        LOG.info("Facade successfully started");
    }

    /**
     * {@inheritDoc}
     */
    public void stop()
    {
        myExecutorService.shutdown();
        LOG.info("Facade successfully stopped");
    }
}
