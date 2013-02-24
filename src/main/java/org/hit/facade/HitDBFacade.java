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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.hit.communicator.Communicator;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Mutation;
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
    
    private final Communicator myCommunicator;
    
    private final RegistryService myRegistryService;
    
    private final NodeID myClientID;
    
    private final Map<String, KeyPartitioner<? extends Comparable<?>>> 
        myTable2Partitoner;
    
    private final Map<DBOperation, FacadeCallback> myDBOperationCallbackMap;
    
    private final Map<String, Pair<Set<NodeID>, FacadeCallback>> 
        myTableCreationCallbackMap;
    
    private final ExecutorService myExecutorService;
    
    /**
     * A helper class that wraps the task of sending mutation to the 
     * server.
     */
    private class DBOperationExecutable implements Runnable
    {
        private final DBOperation myOperation;
        
        private final NodeID myNodeID;
        
        private final FacadeCallback myMutationCallback;

        /**
         * CTOR
         */
        public DBOperationExecutable(NodeID         nodeId, 
                                     DBOperation    operation,
                                     FacadeCallback callback)
        {
            super();
            myNodeID = nodeId;
            myOperation = operation;
            myMutationCallback = callback;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            myDBOperationCallbackMap.put(myOperation, myMutationCallback);
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
    private class TableCreationExecutable implements Runnable
    {
        private final Schema mySchema;
        
        private final FacadeCallback myCallback;

        /**
         * CTOR
         */
        public TableCreationExecutable(Schema         schema,
                                       FacadeCallback callback)
        {
            mySchema = schema;
            myCallback = callback;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Processing request for creating  table with schema " + 
                     mySchema);
            
            Pair<Set<NodeID>, FacadeCallback> facadeCallback = 
                new Pair<Set<NodeID>, FacadeCallback>(
                    new HashSet<>(myRegistryService.getServerNodes()),
                    myCallback);
           
            myTableCreationCallbackMap.put(mySchema.getTableName(), 
                                           facadeCallback);
            
            CreateTableMessage message =
                new CreateTableMessage(myClientID, mySchema);
            
            for (NodeID nodeID : myRegistryService.getServerNodes()) {
                LOG.info("Sending create table request to " + nodeID);
                myCommunicator.sendTo(nodeID, message);
            }
        }
    }
    
    /**
     * A simple class that wraps the functionality of handling the 
     * response from the server.
     */
    private class ResponseHandler implements Runnable
    {
        private final Message myMessage;
        
        /**
         * CTOR
         */
        public ResponseHandler(Message message)
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
                
                LOG.info("Creating table " + createTableResponse.getTableName()
                         + " on " + createTableResponse.getNodeId()
                         + " succedded ");
                
                Pair<Set<NodeID>, FacadeCallback> creationCallback = 
                    myTableCreationCallbackMap.get(
                        createTableResponse.getTableName());
                
                creationCallback.getFirst().remove(
                    createTableResponse.getNodeId());
                
                if (creationCallback.getFirst().isEmpty()) {
                    myTableCreationCallbackMap.remove(
                        createTableResponse.getTableName());
                    creationCallback.getSecond().onTableCreationSuccess(
                        createTableResponse.getTableName());
                }
            }
            else if (myMessage instanceof DBOperationSuccessMessage) {
                DBOperationSuccessMessage dbOperationSuccessMessage = 
                    (DBOperationSuccessMessage) myMessage;
                
                if (dbOperationSuccessMessage.getAppliedDBOperation()
                        instanceof Mutation)
                {
                    FacadeCallback callback = 
                        myDBOperationCallbackMap.get(
                            dbOperationSuccessMessage.getAppliedDBOperation());
                    
                    callback.onDBOperationSuccess(
                            dbOperationSuccessMessage.getAppliedDBOperation());
                        
                }
            }
            else if (myMessage instanceof DBOperationFailureMessage) {
                DBOperationFailureMessage dbOperationFlreMessage = 
                    (DBOperationFailureMessage) myMessage;
                
                if (dbOperationFlreMessage.getDBOperation()
                        instanceof Mutation)
                {
                    FacadeCallback mutationCallback = 
                        myDBOperationCallbackMap.get(
                            dbOperationFlreMessage.getDBOperation());
                    
                    mutationCallback.onDBOperationFailure(
                            dbOperationFlreMessage.getDBOperation(),
                            dbOperationFlreMessage.getMessage(),
                            dbOperationFlreMessage.getException());
                }
            }
        }
    }
    
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
        myDBOperationCallbackMap = new HashMap<>();
        myTableCreationCallbackMap = new HashMap<>();
        myExecutorService = 
            Executors.newFixedThreadPool(4,
                                         new NamedThreadFactory(
                                             HitDBFacade.class));
        
    }

    /**
     * Creates a new table in the database with the given <code>Schema</code>
     */
    public void createTable(Schema schema, FacadeCallback callback)
    {
        try {
            myExecutorService.execute(
                new TableCreationExecutable(schema, callback));
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Applies the mutation to the database.
     */
    public <K extends Comparable<K>> void apply(SingleKeyMutation<K> mutation,
                                                FacadeCallback       callback)
    {
        @SuppressWarnings("unchecked")
        KeyPartitioner<K> partitioner = 
            (KeyPartitioner<K>) myTable2Partitoner.get(mutation.getTableName());
        
        if (partitioner == null) {
            Pair<PartitioningType, KeySpace<?>> keyMeta = 
                myRegistryService.getTableKeyMetaData(mutation.getTableName());
            
            @SuppressWarnings("unchecked")
            KeySpace<K> hashRing = (KeySpace<K>) keyMeta.getSecond();
            
            partitioner = 
                keyMeta.getFirst() == PartitioningType.HASHABLE ?
                    new DistributedPartitioner<K>(hashRing)
                    : new LinearPartitioner<K>(hashRing);
                    
            partitioner.distribute(myRegistryService.getServerNodes() );
            myTable2Partitoner.put(mutation.getTableName(), partitioner);
        }
        
        NodeID serverNode = partitioner.getNode(mutation.getKey());
        myExecutorService.submit(new DBOperationExecutable(
            serverNode, mutation, callback));
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
        
        myCommunicator.addMessageHandler(
             new MessageHandler() {
                 @Override
                 public void handle(Message message)
                 {
                      myExecutorService.execute(
                          new ResponseHandler(message));
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
