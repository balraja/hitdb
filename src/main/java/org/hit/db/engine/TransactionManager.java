/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2013  Balraja Subbiah

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

package org.hit.db.engine;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.UnitID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Mutation;
import org.hit.db.model.Query;
import org.hit.db.model.Schema;
import org.hit.db.transactions.AbstractTransaction;
import org.hit.db.transactions.IDAssigner;
import org.hit.db.transactions.Memento;
import org.hit.db.transactions.PhasedTransactionExecutor;
import org.hit.db.transactions.ReadTransaction;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.TransactionExecutor;
import org.hit.db.transactions.TransactionID;
import org.hit.db.transactions.TransactionResult;
import org.hit.db.transactions.WriteTransaction;
import org.hit.db.transactions.journal.WAL;
import org.hit.event.CreateConsensusLeaderEvent;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.ProposalNotificationResponse;
import org.hit.event.SendMessageEvent;
import org.hit.messages.CreateTableResponseMessage;
import org.hit.messages.DBOperationFailureMessage;
import org.hit.messages.DBOperationSuccessMessage;
import org.hit.time.Clock;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;
import org.hit.zookeeper.ZooKeeperClient;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;

/**
 * Class for managing the execution of transactions.
 * 
 * @author Balraja Subbiah
 */
public class TransactionManager
{
    /** LOGGER */
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(TransactionManager.class);
    
    /**
     * The task that's executed on completion of a <code>Transaction</code>
     */
    private class NotifyResultTask
        implements FutureCallback<TransactionResult>
    {
        private final NodeID myClientID;
        
        private final DBOperation myOperation;
        
        /**
         * CTOR
         */
        public NotifyResultTask(NodeID        clientID,
                                DBOperation   operation)
        {
            super();
            myClientID = clientID;
            myOperation = operation;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            try {
                myEventBus.publish(
                    new SendMessageEvent(
                        Collections.singleton(myClientID),
                        new DBOperationFailureMessage(
                            myServerID,
                            myOperation,
                            exception.getMessage(),
                            exception)));
            }
            catch (EventBusException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(TransactionResult result)
        {
            try {
               
                Message message =
                    result.isCommitted() ?
                        new DBOperationSuccessMessage(
                            myServerID, myOperation, result.getResult())
                        : new DBOperationFailureMessage(
                              myServerID,
                              myOperation,
                              "Failed to apply the transaction on db");
                            
                myEventBus.publish(
                   new SendMessageEvent(
                       Collections.singleton(myClientID),
                       message));
            }
            catch (EventBusException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }
    
    private class ExecutionPhaseCallBack 
        implements FutureCallback<Memento<Boolean>>
    {
        private final UnitID myUnitID;
        
        private final ProposalNotificationEvent myNotification;
        
        /**
         * CTOR
         */
        public ExecutionPhaseCallBack(UnitID unitID,
                                      ProposalNotificationEvent notification)
        {
            super();
            myUnitID = unitID;
            myNotification = notification;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            try {
                if (myNotification != null) {
                    myEventBus.publish(
                        new ProposalNotificationResponse(myNotification, 
                                                         false));
                }
            }
            catch (EventBusException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Memento<Boolean> memento)
        {
            myAwaitingConsensusCommitMap.put(myUnitID, memento);
            
            if (myNotification != null) {
                try {
                    myEventBus.publish(
                        new ProposalNotificationResponse(
                            myNotification, 
                            memento.getPhase().getResult()));
                }
                catch (EventBusException e) {
                    LOG.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
    }
    
    private class CommitPhaseCallBack 
        implements FutureCallback<Memento<TransactionResult>>
    {
        private final ProposalNotificationEvent myNotification;
        
        /**
         * CTOR
         */
        public CommitPhaseCallBack(ProposalNotificationEvent notification)
        {
            super();
            myNotification = notification;
        }
    
        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            try {
                myEventBus.publish(new ProposalNotificationResponse(myNotification, 
                                                                    false));
                DistributedTrnProposal dtp = 
                    (DistributedTrnProposal) myNotification.getProposal();
                
                NotifyResultTask nrt = 
                    new NotifyResultTask(dtp.getClientID(), 
                                         dtp.getNodeToDBOperationMap()
                                            .get(myServerID));
                nrt.onFailure(exception);
            }
            catch (EventBusException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    
        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Memento<TransactionResult> memento)
        {
            DistributedTrnProposal dtp = 
                (DistributedTrnProposal) myNotification.getProposal();
            NotifyResultTask nrt = 
                new NotifyResultTask(dtp.getClientID(), 
                                     dtp.getNodeToDBOperationMap()
                                        .get(myServerID));
            nrt.onSuccess(memento.getPhase().getResult());
        }
    }
   
    private final ListeningExecutorService myExecutor;
    
    private final IDAssigner myIdAssigner;
    
    private final TransactableDatabase myDatabase;
    
    private final Clock myClock;
    
    private final EventBus myEventBus;
    
    private final NodeID myServerID;
    
    private final WAL    myWriteAheadLog;
    
    private final ZooKeeperClient myZooKeeperClient;
    
    private final Map<UnitID, Memento<?>> myAwaitingConsensusCommitMap;
    
    /**
     * CTOR
     */
    @Inject
    public TransactionManager(TransactableDatabase database,
                              Clock                clock,
                              EventBus             eventBus,
                              NodeID               serverID,
                              WAL                  writeAheadLog,
                              ZooKeeperClient      zooKeeperClient)
    {
        myDatabase = database;
        myClock = clock;
        myIdAssigner = new IDAssigner();
        myServerID = serverID;
        myEventBus = eventBus;
        myWriteAheadLog = writeAheadLog;
        myZooKeeperClient = zooKeeperClient;
        myAwaitingConsensusCommitMap = new ConcurrentHashMap<>();
        myExecutor =
            MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(
                    20,
                    new NamedThreadFactory(TransactionManager.class)));
    }
    
    /**
     * Creates a table with the given <code>Schema</code>
     */
    public void createTable(NodeID clientId, Schema schema)
    {
        LOG.info("Received request from " + clientId + " for creating new table"
                 + " with schema " + schema); 
        CreateTableResponseMessage response = null;
        try {
            myDatabase.createTable(schema);
            myZooKeeperClient.addSchema(schema);
            response = 
                new CreateTableResponseMessage(myServerID, 
                                               schema.getTableName(), 
                                               true, 
                                               null);
            LOG.info("Schema for " + schema.getTableName() + " was successfully"
                     + " added to the database");
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            response = 
                new CreateTableResponseMessage(myServerID, 
                                               schema.getTableName(), 
                                               false, 
                                               e.getMessage());
        }
        try {
            myEventBus.publish(new SendMessageEvent(
                                   Collections.singleton(clientId),
                                   response));
        }
        catch (EventBusException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
    
    /**
     * Creates appropriate <code>Transaction<code> to process the mutations/
     * queries on the database.
     */
    public void processOperation(NodeID clientID, DBOperation operation)
    {
        TransactionID id = new TransactionID(myIdAssigner.getTransactionID());
        
        AbstractTransaction transaction =
            operation instanceof Mutation ?
                new WriteTransaction(
                    id, myDatabase, myClock, (Mutation) operation)
                : new ReadTransaction(
                    id, myDatabase, myClock, (Query) operation);
                    
        ListenableFuture<TransactionResult> future =
            myExecutor.submit(new TransactionExecutor(transaction,
                                                      myWriteAheadLog));
        Futures.addCallback(future,
                            new NotifyResultTask(clientID, operation));
    }
    
    /**
     * Creates appropriate <code>Transaction<code>s to process the mutations/
     * queries to be performed on multitude of nodes of the database.
     */
    public void processOperation(NodeID                   clientID, 
                                 Map<NodeID, DBOperation> operations)
    {
        Set<NodeID> acceptors = 
            Sets.difference(operations.keySet(), 
                            Collections.singleton(myServerID));
        UnitID unitID = new DistributedTrnConsensusID(clientID);
        try {
            myEventBus.publish(new CreateConsensusLeaderEvent(unitID, acceptors));
        }
        catch (EventBusException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        
        TransactionID id = new TransactionID(myIdAssigner.getTransactionID());
        DBOperation operation = operations.get(myServerID);
        
        AbstractTransaction transaction =
            operation instanceof Mutation ?
                new WriteTransaction(
                    id, myDatabase, myClock, (Mutation) operation)
                : new ReadTransaction(
                    id, myDatabase, myClock, (Query) operation);
                    
        ListenableFuture<Memento<Boolean>> future =
            myExecutor.submit(
               new PhasedTransactionExecutor<Boolean>(
                   transaction,
                   myWriteAheadLog, 
                   new PhasedTransactionExecutor.ExecutionPhase(transaction)));
        
        Futures.addCallback(future,
                            new ExecutionPhaseCallBack(unitID, null));
    }
    
    /**
     * Creates appropriate <code>Transaction<code>s to process the mutations/
     * queries to be performed on multitude of nodes of the database.
     */
    public void processOperation(ProposalNotificationEvent pne)
    {
        DistributedTrnProposal distributedTrnProposal = 
            (DistributedTrnProposal) pne.getProposal();
        
        TransactionID id = new TransactionID(myIdAssigner.getTransactionID());
        DBOperation operation = 
            distributedTrnProposal.getNodeToDBOperationMap()
                                  .get(myServerID);
        
        if (operation != null) {
        
            AbstractTransaction transaction =
                operation instanceof Mutation ?
                    new WriteTransaction(
                        id, myDatabase, myClock, (Mutation) operation)
                    : new ReadTransaction(
                        id, myDatabase, myClock, (Query) operation);
           
           if (pne.isCommitNotification()) {
               Memento<?> memento = 
                   myAwaitingConsensusCommitMap.get(
                       pne.getProposal().getUnitID());
               
               if (memento != null) {
                   ListenableFuture<Memento<TransactionResult>> future =
                       myExecutor.submit(
                          new PhasedTransactionExecutor<TransactionResult>(
                              memento));
                   
                   Futures.addCallback(future, 
                                       new CommitPhaseCallBack(pne));
               }
           }
           else {         
                ListenableFuture<Memento<Boolean>> future =
                    myExecutor.submit(
                       new PhasedTransactionExecutor<Boolean>(
                           transaction,
                           myWriteAheadLog, 
                           new PhasedTransactionExecutor.ExecutionPhase(
                               transaction)));

                Futures.addCallback(future,
                                    new ExecutionPhaseCallBack(
                                        distributedTrnProposal.getUnitID(),
                                        pne));
           }
        }
    }
}
