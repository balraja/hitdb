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
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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
import org.hit.event.DBOperationFailureMessage;
import org.hit.event.DBOperationSuccessMessage;
import org.hit.event.PerformDBOperationMessage;
import org.hit.registry.RegistryService;
import org.hit.topology.Topology;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;
import org.hit.util.NamedThreadFactory;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Defines the contract for a class that acts as a client to the database.
 * Primarily this class reads the commands/statements from the command line
 * and after parsing the same sends it down to the servers.
 * 
 * @author Balraja Subbiah
 */
public class HitFacade implements Application
{
    /**
     * The main method that launches the client.
     */
    public static void main(String[] args)
    {
        ApplicationLauncher launcher = new ApplicationLauncher(new HitFacade());
        launcher.launch();
    }
    
    private final Communicator myCommunicator;
    
    private final RegistryService myRegistryService;
    
    private final NodeID myClientID;
    
    private final Map<String,  KeyPartitioner<? extends Comparable<?>>> 
        myTable2Partitoner;
    
    private final Map<Mutation, MutationCallback> myMutationCallbackMap;
    
    private final ExecutorService myExecutorService;
    
    private class SendMutationRunnable implements Runnable
    {
        private final Mutation myMutation;
        
        private final NodeID myNodeID;
        
        private final MutationCallback myMutationCallback;

        /**
         * CTOR
         */
        public SendMutationRunnable(NodeID nodeId, 
                                    Mutation mutation,
                                    MutationCallback callback)
        {
            super();
            myNodeID = nodeId;
            myMutation = mutation;
            myMutationCallback = callback;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            myMutationCallbackMap.put(myMutation, myMutationCallback);
            myCommunicator.sendTo(myNodeID, 
                                  new PerformDBOperationMessage(
                                      myClientID, myMutation));
            
        }
    }
    
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
            if (myMessage instanceof DBOperationSuccessMessage) {
                DBOperationSuccessMessage dbOperationSuccessMessage = 
                    (DBOperationSuccessMessage) myMessage;
                
                if (dbOperationSuccessMessage.getAppliedDBOperation()
                        instanceof Mutation)
                {
                    MutationCallback mutationCallback = 
                        myMutationCallbackMap.get(
                            dbOperationSuccessMessage.getAppliedDBOperation());
                    
                    mutationCallback.onSuccess(
                        (Mutation) 
                            dbOperationSuccessMessage.getAppliedDBOperation());
                        
                }
            }
            else if (myMessage instanceof DBOperationFailureMessage) {
                DBOperationFailureMessage dbOperationFlreMessage = 
                    (DBOperationFailureMessage) myMessage;
                
                if (dbOperationFlreMessage.getDBOperation()
                        instanceof Mutation)
                {
                    MutationCallback mutationCallback = 
                        myMutationCallbackMap.get(
                            dbOperationFlreMessage.getDBOperation());
                    
                    mutationCallback.onSuccess(
                        (Mutation) 
                            dbOperationFlreMessage.getDBOperation());
                        
                }
            }

        }
        
    }
    
    /**
     * CTOR
     */
    public HitFacade()
    {
        Injector injector = Guice.createInjector(new HitFacadeModule());
        myCommunicator = injector.getInstance(Communicator.class);
        myRegistryService = injector.getInstance(RegistryService.class);
        myClientID = injector.getInstance(NodeID.class);
        myTable2Partitoner = new HashMap<>();
        myMutationCallbackMap = new HashMap<>();
        myExecutorService = 
            Executors.newFixedThreadPool(4,
                                         new NamedThreadFactory(HitFacade.class));
    }

    /**
     * Creates a new table in the database with the given <code>Schema</code>
     */
    public void createTable(Schema schema)
    {
        Topology topology = myRegistryService.getTopology();
        CreateTableMessage message =
            new CreateTableMessage(myClientID, schema);
        
        for (NodeID nodeID : topology.getNodes()) {
            myCommunicator.sendTo(nodeID, message);
        }
    }
    
    public <K extends Comparable<K>>void apply(SingleKeyMutation<K> mutation,
                                               MutationCallback callback)
    {
        @SuppressWarnings("unchecked")
        KeyPartitioner<K> partitioner = 
            (KeyPartitioner<K>) myTable2Partitoner.get(mutation.getTableName());
        
        if (partitioner == null) {
            Schema schema = myRegistryService.getSchema(mutation.getTableName());
            
            @SuppressWarnings("unchecked")
            KeySpace<K> hashRing = (KeySpace<K>) schema.getHashRing();
            
            partitioner = 
                schema.getKeyPartitioningType() == PartitioningType.HASHABLE ?
                    new DistributedPartitioner<K>(hashRing)
                    : new LinearPartitioner<K>(hashRing);
                    
            partitioner.distribute(myRegistryService.getTopology().getNodes());
            myTable2Partitoner.put(mutation.getTableName(), partitioner);
        }
        
        NodeID serverNode = partitioner.getNode(mutation.getKey());
        myExecutorService.execute(new SendMutationRunnable(
            serverNode, mutation, callback));
    }
    
    /**
     * {@inheritDoc}
     */
    public void start()
    {
        myCommunicator.addMessageHandler(
            new MessageHandler() {
                @Override
                public void handle(Message message)
                {
                     myExecutorService.execute(
                         new ResponseHandler(message));
                }
            });
    }
    
    /**
     * {@inheritDoc}
     */
    public void stop()
    {
        myExecutorService.shutdown();
    }
}
