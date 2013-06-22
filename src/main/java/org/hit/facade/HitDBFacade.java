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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.hit.communicator.Communicator;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Query;
import org.hit.db.model.Queryable;
import org.hit.db.model.Schema;
import org.hit.db.model.mutations.SingleKeyMutation;
import org.hit.db.query.merger.QueryMerger;
import org.hit.db.query.merger.SimpleQueryMerger;
import org.hit.db.query.operators.QueryBuilder;
import org.hit.db.query.operators.QueryBuildingException;
import org.hit.db.query.parser.HitSQLLexer;
import org.hit.db.query.parser.HitSQLParser;
import org.hit.db.query.parser.HitSQLTree;
import org.hit.di.HitFacadeModule;
import org.hit.key.Partition;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.CreateTableResponseMessage;
import org.hit.messages.DBOperationFailureMessage;
import org.hit.messages.DBOperationMessage;
import org.hit.messages.DBOperationSuccessMessage;
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
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HitDBFacade.class);

    private static final String TABLE_CREATION_FAILURE =
        "Creation of table %s on host %s failed";

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
        @SuppressWarnings("unchecked")
        @Override
        public void run()
        {
            if (myMessage instanceof CreateTableResponseMessage) {
                final CreateTableResponseMessage createTableResponse =
                    (CreateTableResponseMessage) myMessage;

                final SettableFuture<NodeID> tableCreationFuture =
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
                final DBOperationSuccessMessage dbOperationSuccessMessage =
                   (DBOperationSuccessMessage) myMessage;
                final DBOperation appliedOperation =
                   dbOperationSuccessMessage.getAppliedDBOperation();

                if (appliedOperation instanceof WrappedMutation) {

                    final long id = 
                        ((WrappedMutation) appliedOperation).getID();
                    final SettableFuture<DBOperationResponse> future  =
                        myMutationIDToFutureMap.get(Long.valueOf(id));

                    future.set(new DBOperationResponse(
                        appliedOperation,
                        dbOperationSuccessMessage.getResult()));
                }
                else if (appliedOperation instanceof WrappedQuery) {
                    
                    final long id = 
                        ((WrappedQuery) appliedOperation).getOperationId();
                    final SettableFuture<Pair<NodeID, Collection<Queryable>>>
                        future = myQueryToMergableFutureMap.get(id);
                    future.set(new Pair<>(
                        dbOperationSuccessMessage.getNodeId(),
                        (Collection<Queryable>) 
                            dbOperationSuccessMessage.getResult()));
                }
            }
            else if (myMessage instanceof DBOperationFailureMessage) {
                final DBOperationFailureMessage dbOperationFailure =
                    (DBOperationFailureMessage) myMessage;

                final DBOperation appliedOperation =
                     dbOperationFailure.getDBOperation();

                if (appliedOperation instanceof WrappedMutation) {

                    final long id = ((WrappedMutation) appliedOperation).getID();
                    final SettableFuture<DBOperationResponse> future  =
                        myMutationIDToFutureMap.get(Long.valueOf(id));
                    future.setException(
                        dbOperationFailure.getException());
                }
                else if (appliedOperation instanceof WrappedQuery) {
                    
                    final long id = 
                        ((WrappedQuery) appliedOperation).getOperationId();
                    final SettableFuture<Pair<NodeID, Collection<Queryable>>>
                        future = myQueryToMergableFutureMap.get(id);
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

            myMutationIDToFutureMap.put(myOperationID, mySettableFuture);

            myCommunicator.sendTo(myNodeID,
                                  new DBOperationMessage(
                                                         myClientID,
                                                         myOperation));
        }
    }
    
    private class SubmitQueryTask implements Runnable
    {
        private final SettableFuture<QueryResponse> myCLientFuture;
        
        private final Query myQuery;
        
        /**
         * CTOR
         */
        public SubmitQueryTask(SettableFuture<QueryResponse> cLientFuture,
                               Query query)
        {
            super();
            myCLientFuture = cLientFuture;
            myQuery = query;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            SettableFuture<Pair<NodeID, Collection<Queryable>>>
                resultFuture = SettableFuture.create();
            Long operationId = 
                Long.valueOf(myOperationsCount.incrementAndGet());
            Set<NodeID> serverNodes = 
                new HashSet<>(myRegistryService.getServerNodes());
            Futures.addCallback(
                resultFuture,
                new QueryResponserHandler(
                    operationId, 
                    serverNodes, 
                    new SimpleQueryMerger(),
                    myCLientFuture));
            
            myQueryToMergableFutureMap.put(operationId, resultFuture);
            
            for (NodeID server : serverNodes) {
                myCommunicator.sendTo(server,
                                      new DBOperationMessage(myClientID, myQuery));
            }
        }
    }
    
    private class QueryResponserHandler 
        implements FutureCallback<Pair<NodeID, Collection<Queryable>>>
    {
        private final Set<NodeID> myServerNodes;
        
        private final QueryMerger myQueryMerger;
        
        private final SettableFuture<QueryResponse> myClientFuture;
        
        private final Long myOperationId;

        /**
         * CTOR
         */
        public QueryResponserHandler(
            Long operationId,
            Set<NodeID> serverNodes,
            QueryMerger queryMerger,
            SettableFuture<QueryResponse> clientFuture)
        {
            super();
            myOperationId = operationId;
            myServerNodes = serverNodes;
            myQueryMerger = queryMerger;
            myClientFuture = clientFuture;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            myClientFuture.setException(exception);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Pair<NodeID, Collection<Queryable>> result)
        {
            myServerNodes.remove(result.getFirst());
            myQueryMerger.addPartialResult(result.getSecond());
            if (myServerNodes.isEmpty()) {
                myClientFuture.set(new QueryResponse(
                    myQueryMerger.getMergedResult()));
                myQueryToMergableFutureMap.remove(myOperationId);
            }
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

            final List<NodeID> serverNodes = myRegistryService.getServerNodes();
            final TableCreationResponseHandler tableCreationHandler =
                new TableCreationResponseHandler(
                    mySchema, myFuture, serverNodes);
            final SettableFuture<NodeID> tableCreationFuture = 
                SettableFuture.create();
            Futures.addCallback(tableCreationFuture, tableCreationHandler);
            myPhasedTableCreationFutureMap.put(mySchema.getTableName(),
                                               tableCreationFuture);

            final CreateTableMessage message =
                new CreateTableMessage(myClientID, mySchema);
            final NodeID serverNode = serverNodes.get(0);
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
                final SettableFuture<NodeID> tableCreationFuture
                    = SettableFuture.create();
                Futures.addCallback(tableCreationFuture, this);
                final CreateTableMessage message =
                    new CreateTableMessage(myClientID, myTableSchema);
                final NodeID serverNode = myServerNodes.get(0);
                myCommunicator.sendTo(serverNode, message);
                myPhasedTableCreationFutureMap.put(
                    myTableSchema.getTableName(), tableCreationFuture);
            }
        }
    }
    
    private final NodeID myClientID;

    private final Communicator myCommunicator;

    private final ListeningExecutorService myExecutorService;

    private final Map<Long, SettableFuture<DBOperationResponse>>
        myMutationIDToFutureMap;
    
    private final Map<Long, SettableFuture<Pair<NodeID, Collection<Queryable>>>>
        myQueryToMergableFutureMap;

    private final AtomicLong myOperationsCount;

    private final Map<String, SettableFuture<NodeID>>
        myPhasedTableCreationFutureMap;

    private final RegistryService myRegistryService;

    private final Map<String, Partition<? extends Comparable<?>>>
        myTable2Partitoner;
    

    /**
     * CTOR
     */
    public HitDBFacade()
    {
        final Injector injector = Guice.createInjector(new HitFacadeModule());
        myCommunicator = injector.getInstance(Communicator.class);
        myRegistryService = injector.getInstance(RegistryService.class);
        myClientID = injector.getInstance(NodeID.class);
        myTable2Partitoner = new HashMap<>();
        myMutationIDToFutureMap = new HashMap<>();
        myOperationsCount = new AtomicLong(0L);
        myPhasedTableCreationFutureMap = new HashMap<>();
        myQueryToMergableFutureMap = new HashMap<>();

        myExecutorService =
            MoreExecutors.listeningDecorator(
                Executors.newSingleThreadExecutor(
                    new NamedThreadFactory(HitDBFacade.class)));

    }
    
    /**
     * A helper method to query the database.
     */
    public ListenableFuture<QueryResponse> queryDB(String query)
        throws QueryBuildingException, RecognitionException
    {
        ANTLRStringStream fs = new ANTLRStringStream(query);
        HitSQLLexer lex = new HitSQLLexer(fs);
        TokenRewriteStream tokens = new TokenRewriteStream(lex);
        HitSQLParser parser = new HitSQLParser(tokens);
        
        HitSQLParser.select_statement_return result = 
            parser.select_statement();
        
        CommonTree t = (CommonTree) result.getTree();
        CommonTreeNodeStream nodeStream = new CommonTreeNodeStream(t);
        HitSQLTree tree = new HitSQLTree(nodeStream);
        tree.select_statement();
        
        QueryBuilder builder = new QueryBuilder(tree.getQueryAttributes());
        return queryDB(builder.buildQuery());
    }
    
    /**
     * A helper method to query the database.
     */
    public ListenableFuture<QueryResponse> queryDB(Query query)
    {
        SettableFuture<QueryResponse> queryResponse = SettableFuture.create();
        myExecutorService.submit(new SubmitQueryTask(queryResponse, query));
        return queryResponse;
    }

    /**
     * Applies the mutation to the database.
     */
    public <K extends Comparable<K>>
        ListenableFuture<DBOperationResponse> apply(
            SingleKeyMutation<K> mutation, String tableName)
    {
        @SuppressWarnings("unchecked")
        Partition<K> partitioner =
            (Partition<K>) myTable2Partitoner.get(tableName);

        if (partitioner == null) {
            partitioner =
                myRegistryService.getTablePartitioner(tableName);
            partitioner.distribute(myRegistryService.getServerNodes() );
            myTable2Partitoner.put(tableName, partitioner);
        }

        final NodeID serverNode = partitioner.getNode(mutation.getKey());
        final SettableFuture<DBOperationResponse> futureResponse =
            SettableFuture.create();
        final long id = myOperationsCount.getAndIncrement();
        final WrappedMutation wrappedMutation = 
            new WrappedMutation(id, mutation);
        myExecutorService.submit(new SubmitDBOperationTask(
            serverNode, wrappedMutation, futureResponse, id));
        return futureResponse;
    }

    /**
     * Creates a new table in the database with the given <code>Schema</code>
     */
    public ListenableFuture<TableCreationResponse> createTable(Schema schema)
    {
        final SettableFuture<TableCreationResponse> clientFuture =
            SettableFuture.create();

        myExecutorService.submit(new SubmitTableCreationTask(
            clientFuture, schema));

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
