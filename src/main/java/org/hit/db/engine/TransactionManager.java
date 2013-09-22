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

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.concurrent.UnboundedLocklessQueue;
import org.hit.concurrent.UnboundedLocklessQueue.DataException;
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
import org.hit.db.transactions.Registry;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.Transaction;
import org.hit.db.transactions.TransactionResult;
import org.hit.db.transactions.WriteTransaction;
import org.hit.db.transactions.journal.WAL;
import org.hit.event.CreateConsensusLeaderEvent;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.ProposalNotificationResponse;
import org.hit.event.SendMessageEvent;
import org.hit.messages.DBOperationFailureMessage;
import org.hit.messages.DBOperationSuccessMessage;
import org.hit.time.Clock;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

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
    /**
     * A simple class to implement a structure for capturing the 
     * client information.
     */
    private static class ClientInfo
    {
        private final NodeID myClientID;
        
        private final long myClientSequenceNumber;

        /**
         * CTOR
         */
        public ClientInfo(NodeID clientID, long clientSequenceNumber)
        {
            super();
            myClientID = clientID;
            myClientSequenceNumber = clientSequenceNumber;
        }

        /**
         * Returns the value of clientID
         */
        public NodeID getClientID()
        {
            return myClientID;
        }

        /**
         * Returns the value of clientSequenceNumber
         */
        public long getClientSequenceNumber()
        {
            return myClientSequenceNumber;
        }
    }
    
    private static class DistributedTrnInfo
    {
        private final AbstractTransaction myTransaction;
        
        private final UnitID  myConsensusID;
        
        private final ProposalNotificationEvent myPne;

        /**
         * CTOR
         */
        public DistributedTrnInfo(AbstractTransaction transaction,
                                  UnitID consensusID,
                                  ProposalNotificationEvent pne)
        {
            super();
            myTransaction = transaction;
            myConsensusID = consensusID;
            myPne = pne;
        }

        /**
         * Returns the value of transaction
         */
        public AbstractTransaction getTransaction()
        {
            return myTransaction;
        }

        /**
         * Returns the value of consensusID
         */
        public UnitID getConsensusID()
        {
            return myConsensusID;
        }

        /**
         * Returns the value of pne
         */
        public ProposalNotificationEvent getPne()
        {
            return myPne;
        }
    }
    
    private class StandardExecutionCallback
        implements FutureCallback<Memento<Boolean>>
    {
        private final long myTransactionID;
        
        /**
         * CTOR
         */
        public StandardExecutionCallback(long transactionID)
        {
            myTransactionID = transactionID;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            ClientInfo clientInfo = myTrnToClientMap.get(myTransactionID);
            if (clientInfo != null) {
                myEventBus.publish(
                   new SendMessageEvent(
                       Collections.singleton(
                           clientInfo.getClientID()),
                       new DBOperationFailureMessage(
                           myServerID,
                           clientInfo.getClientSequenceNumber(),
                           exception.getMessage(),
                           exception)));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Memento<Boolean> result)
        {
            if (result.getPhase().getResult()) {
                // We can commit the changes
                ListenableFuture<Memento<TransactionResult>> future =
                                myExecutor.submit(
                                   new PhasedTransactionExecutor<TransactionResult>(
                                       result));

                Futures.addCallback(future,
                                    new StandardCommitCallback(
                                        myTransactionID));
            }
            else {
                // We have to wait.
                myAwaitingCommitMap.put(Long.valueOf(myTransactionID), 
                                        result);
            }
        }
    }
    
    private class StandardCommitCallback
        implements FutureCallback<Memento<TransactionResult>>
    {
        private final long myTransactionID;
        
        /**
         * CTOR
         */
        public StandardCommitCallback(long transactionID)
        {
            super();
            myTransactionID = transactionID;
        }
        
        /**
         * Schedules the next set of transactions which are dependent
         * on this transaction.
         */
        private void scheduleNextTransactions()
        {
            TLongSet toBeProcessedTransactions = 
                Registry.freeDependentTransactionsOn(myTransactionID);
            if (!toBeProcessedTransactions.isEmpty()) {
                myExecutor.submit(new ScheduleTransactionCommitTask(
                    toBeProcessedTransactions));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            ClientInfo clientInfo = myTrnToClientMap.get(myTransactionID);
            if (clientInfo != null) {
                    myEventBus.publish(
                       new SendMessageEvent(
                           Collections.singleton(
                               clientInfo.getClientID()),
                           new DBOperationFailureMessage(
                               myServerID,
                               clientInfo.getClientSequenceNumber(),
                               exception.getMessage(),
                               exception)));
            }
            // XXX Abort all transactions that is dependent 
            // on this transaction.
        }
    
        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Memento<TransactionResult> result)
        {
            ClientInfo clientInfo = myTrnToClientMap.get(myTransactionID);
            if (clientInfo != null) {
                myEventBus.publish(myDatabase.getStatistics());
                Message message =
                    result.getPhase().getResult().isCommitted() ?
                        new DBOperationSuccessMessage(
                            myServerID,
                            clientInfo.getClientSequenceNumber(), 
                            result.getPhase().getResult().getResult())
                        : new DBOperationFailureMessage(
                              myServerID,
                              clientInfo.getClientSequenceNumber(),
                              "Failed to apply the transaction on db");

                myEventBus.publish(
                   new SendMessageEvent(
                       Collections.singleton(clientInfo.getClientID()),
                       message));
            }
            scheduleNextTransactions();
        }
    }
    
    private class CommitPhaseCallBack
        extends StandardCommitCallback
    {
        private final ProposalNotificationEvent myNotification;

        /**
         * CTOR
         */
        public CommitPhaseCallBack(long transactionID,
                                   ProposalNotificationEvent notification)
        {
            super(transactionID);
            myNotification = notification;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            myEventBus.publish(new ProposalNotificationResponse(
                                   myNotification,
                                   false));
            super.onFailure(exception);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(Memento<TransactionResult> memento)
        {
            super.onSuccess(memento);
            long id = memento.getTransaction().getTransactionID();
            myDistributedTrnInfoMap.remove(id);
            myDatabase.unlock(id);
            if (!myDistributedTrnInfoMap.isEmpty()) {
                // XXX shud this be the only way for scheduling 
                // waiting distributed transactions
                long nextID = myDistributedTrnInfoMap.keys()[0];
                if (myDatabase.lock(nextID)) {
                    Registry.addDependencyToAll(nextID);
                }
            }
            List<AbstractTransaction> queued = 
                new ArrayList<>(myQueuedTransactions);
            myQueuedTransactions.clear();
            myExecutor.submit(new ScheduleTransactionExecutionTask(queued));
        }
    }

    private class ExecutionPhaseCallBack
        implements FutureCallback<Memento<Boolean>>
    {
        private final ProposalNotificationEvent myNotification;

        private final UnitID myUnitID;

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
            if (myNotification != null) {
                myEventBus.publish(
                    new ProposalNotificationResponse(myNotification,
                                                     false));
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
                myEventBus.publish(
                    new ProposalNotificationResponse(
                        myNotification,
                        memento.getPhase().getResult()));
            }
        }
    }
    
    private class ScheduleTransactionExecutionTask implements Runnable
    {
        private final List<AbstractTransaction> myList;
        
        /**
         * CTOR
         */
        public ScheduleTransactionExecutionTask(List<AbstractTransaction> list)
        {
            super();
            myList = list;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            for (AbstractTransaction transaction : myList) {
                ListenableFuture<Memento<Boolean>> future =
                        myExecutor.submit(
                           new PhasedTransactionExecutor<Boolean>(
                               transaction,
                               myWriteAheadLog,
                               new PhasedTransactionExecutor.ExecutionPhase(
                                   transaction)));
            
                Futures.addCallback(future, 
                                    new StandardExecutionCallback(
                                        transaction.getTransactionID()));
                    
            }
        }
    }

    
    private class ScheduleTransactionCommitTask implements Runnable
    {
        private final TLongSet myTransactionSet;
        
        /**
         * CTOR
         */
        public ScheduleTransactionCommitTask(TLongSet transactionSet)
        {
            super();
            myTransactionSet = transactionSet;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            for (long transactionID : myTransactionSet.toArray()) {
                
                @SuppressWarnings("unchecked")
                Memento<Boolean> preservedTrnState = 
                    (Memento<Boolean>) 
                        myAwaitingCommitMap.get(Long.valueOf(transactionID));
                
                if (preservedTrnState != null) {
                    // We can commit the changes
                    ListenableFuture<Memento<TransactionResult>> future =
                                myExecutor.submit(
                                   new PhasedTransactionExecutor<TransactionResult>(
                                       preservedTrnState));
    
                    Futures.addCallback(future,
                                        new StandardCommitCallback(
                                            preservedTrnState
                                                  .getTransaction()
                                                  .getTransactionID()));
                }
                else {
                    DistributedTrnInfo info = 
                        myDistributedTrnInfoMap.get(transactionID);
                    
                    if (myDatabase.canProcess(transactionID))
                    {
                        // Now schedule a distributed transaction to 
                        // execute.
                        ListenableFuture<Memento<Boolean>> future =
                            myExecutor.submit(
                               new PhasedTransactionExecutor<Boolean>(
                                   info.getTransaction(),
                                   myWriteAheadLog,
                                   new PhasedTransactionExecutor.ExecutionPhase(
                                       info.getTransaction())));
                            
                        Futures.addCallback(future,
                                            new ExecutionPhaseCallBack(
                                                info.getConsensusID(), 
                                                info.getPne()));
                            
                    }
                }
            }
        }
    }

    /** LOGGER */
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(TransactionManager.class);

    private final Map<UnitID, Memento<?>> myAwaitingConsensusCommitMap;
    
    private final Map<Long, Memento<?>> myAwaitingCommitMap;
    
    private final TLongObjectMap<ClientInfo> myTrnToClientMap;

    private final Clock myClock;

    private final TransactableDatabase myDatabase;

    private final EventBus myEventBus;

    private final ListeningExecutorService myExecutor;

    private final IDAssigner myIdAssigner;

    private final NodeID myServerID;

    private final WAL    myWriteAheadLog;
    
    private final List<AbstractTransaction> myQueuedTransactions; 
    
    private final TLongObjectMap<DistributedTrnInfo> myDistributedTrnInfoMap;

    /**
     * CTOR
     */
    @Inject
    public TransactionManager(TransactableDatabase database,
                              Clock                clock,
                              EventBus             eventBus,
                              NodeID               serverID,
                              WAL                  writeAheadLog)
    {
        myDatabase = database;
        myClock = clock;
        myIdAssigner = new IDAssigner();
        myServerID = serverID;
        myEventBus = eventBus;
        myWriteAheadLog = writeAheadLog;
        myAwaitingConsensusCommitMap = new ConcurrentHashMap<>();
        myAwaitingCommitMap = new ConcurrentHashMap<>();
        myTrnToClientMap = new TLongObjectHashMap<>();
        myQueuedTransactions = new CopyOnWriteArrayList<>();
        myDistributedTrnInfoMap = new TLongObjectHashMap<>();
        myExecutor =
            MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(
                    20,
                    new NamedThreadFactory(TransactionManager.class)));
    }

    /**
     * Creates a table with the given <code>Schema</code>
     */
    public boolean createTable(Schema schema)
    {
        try {
            myDatabase.createTable(schema);
            LOG.info("Schema for " + schema.getTableName() +
                     " has been successfully added to the database");
            return true;
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Creates appropriate <code>Transaction<code> to process the mutations/
     * queries on the database.
     */
    public void processOperation(NodeID clientID,
                                 DBOperation operation,
                                 long sequenceNumber)
    {
        long id = myIdAssigner.getTransactionID();
        ClientInfo clientInfo = new ClientInfo(clientID, sequenceNumber);
        myTrnToClientMap.put(id, clientInfo);
        
        AbstractTransaction transaction =
            operation instanceof Mutation ?
                new WriteTransaction(
                    id, myDatabase, myClock, (Mutation) operation)
                : new ReadTransaction(
                    id, myDatabase, myClock, (Query) operation);
        
        if (myDatabase.canProcess(id)) {

            ListenableFuture<Memento<Boolean>> future =
                myExecutor.submit(
                   new PhasedTransactionExecutor<Boolean>(
                       transaction,
                       myWriteAheadLog,
                       new PhasedTransactionExecutor.ExecutionPhase(transaction)));
    
            Futures.addCallback(future, 
                                new StandardExecutionCallback(id));
        }
        else {
            myQueuedTransactions.add(transaction);
        }
    }

    /**
     * Creates appropriate <code>Transaction<code>s to process the mutations/
     * queries to be performed on multitude of nodes of the database.
     */
    public void processOperation(NodeID                   clientID,
                                 Map<NodeID, DBOperation> operations)
    {
        // XXX Send proper seq number from the client side and create
        // appropriate ClientInfo
        
        Set<NodeID> acceptors =
            Sets.difference(operations.keySet(),
                            Collections.singleton(myServerID));
        UnitID unitID = new DistributedTrnConsensusID(clientID);
        myEventBus.publish(new CreateConsensusLeaderEvent(unitID, 
                                                          acceptors));

        long id = myIdAssigner.getTransactionID();
        DBOperation operation = operations.get(myServerID);

        AbstractTransaction transaction =
            operation instanceof Mutation ?
                new WriteTransaction(
                    id, myDatabase, myClock, (Mutation) operation)
                : new ReadTransaction(
                    id, myDatabase, myClock, (Query) operation);
        
        myDistributedTrnInfoMap.put(id,
                                    new DistributedTrnInfo(transaction, 
                                                           unitID,
                                                           null));
        if (myDatabase.lock(id)) {
            // The new distributed transaction will be 
            // dependent on all other transactions to complete.
            Registry.addDependencyToAll(id);
        }
    }

    /**
     * Creates appropriate <code>Transaction<code>s to process the mutations/
     * queries to be performed on multitude of nodes of the database.
     */
    public void processOperation(ProposalNotificationEvent pne)
    {
        DistributedTrnProposal distributedTrnProposal =
            (DistributedTrnProposal) pne.getProposal();
        DBOperation operation =
            distributedTrnProposal.getNodeToDBOperationMap()
                                  .get(myServerID);

        if (operation != null) {

           if (pne.isCommitNotification()) {
               Memento<?> memento =
                   myAwaitingConsensusCommitMap.get(
                       pne.getProposal().getUnitID());

               if (memento != null) {
                   ListenableFuture<Memento<TransactionResult>> future =
                       myExecutor.submit(
                          new PhasedTransactionExecutor<TransactionResult>(
                              memento));

                   // TODO Fix this, we are assuming client will take care
                   // of merging the commits.
                   Futures.addCallback(future,
                                       new CommitPhaseCallBack(
                                           memento.getTransaction()
                                                  .getTransactionID(),
                                           pne));
               }
           }
           else {

               long id = myIdAssigner.getTransactionID();
               AbstractTransaction transaction =
                   operation instanceof Mutation ?
                       new WriteTransaction(
                           id, myDatabase, myClock, (Mutation) operation)
                       : new ReadTransaction(
                           id, myDatabase, myClock, (Query) operation);

               myDistributedTrnInfoMap.put(id,
                                           new DistributedTrnInfo(
                                               transaction, 
                                               distributedTrnProposal.getUnitID(),
                                               pne));
               
               if (myDatabase.lock(id)) {
                   // The new distributed transaction will be 
                   // dependent on all other transactions to complete.
                   Registry.addDependencyToAll(id);
               }
           }
        }
    }
}
