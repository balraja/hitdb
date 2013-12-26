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

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.UnitID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.DatabaseException;
import org.hit.db.model.Mutation;
import org.hit.db.model.Query;
import org.hit.db.model.Schema;
import org.hit.db.model.mutations.MutationWrapper;
import org.hit.db.transactions.AbstractTransaction;
import org.hit.db.transactions.IDAssigner;
import org.hit.db.transactions.Memento;
import org.hit.db.transactions.PhasedTransactionExecutor;
import org.hit.db.transactions.ReadTransaction;
import org.hit.db.transactions.Registry;
import org.hit.db.transactions.ReplicatedWriteTransaction;
import org.hit.db.transactions.ReplicationExecutor;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.TransactionResult;
import org.hit.db.transactions.WriteTransaction;
import org.hit.event.ConsensusRequestEvent;
import org.hit.event.ConsensusResponseEvent;
import org.hit.event.CreateConsensusLeaderEvent;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.ProposalNotificationResponse;
import org.hit.event.SendMessageEvent;
import org.hit.messages.DBOperationFailureMessage;
import org.hit.messages.DBOperationSuccessMessage;
import org.hit.messages.DataLoadResponse;
import org.hit.time.Clock;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;
import org.hit.util.Range;

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
     * Defines the contract for a workflow that captures the state of
     * transaction execution.
     */
    private static interface WorkFlow
    {
        /** Returns the transaction id corresponding to this work flow */
        public long getTransactionID();
        
        /** Starts workflow processing */
        public void start();
        
        /** Initiates commit phase for a transaction */
        public void initiateCommit();
        
        /** The actual method via which workflow responds to an event */
        public void respondTO(Object event);
    }
    
    /**
     * Defines an abstract implementation of {@link WorkFlow}.
     */
    private abstract class AbstractWokflow implements WorkFlow
    {
        /**
         * Schedules the next set of transactions which are dependent
         * on this transaction.
         */
        protected void scheduleNextTransactions(long transactionID)
        {
            TLongSet toBeProcessedTransactions = 
                Registry.freeDependentTransactionsOn(transactionID);
            
            if (!toBeProcessedTransactions.isEmpty()) {
                myExecutor.submit(new ScheduleTransactionCommitTask(
                    toBeProcessedTransactions));
            }
        }
    }
    
    /**
     * Defines a workflow where the transaction is performed only 
     * on the data present in this server.
     */
    private class SimpleWorkflow extends AbstractWokflow
    {
        private final ClientInfo myClientInfo;
        
        private final AbstractTransaction myTransaction;
        
        private boolean myExecutionPahse;
        
        private Memento<Boolean> myMemento;

        /**
         * CTOR
         */
        public SimpleWorkflow(ClientInfo clientInfo, 
                              AbstractTransaction transaction)
        {
            super();
            myClientInfo = clientInfo;
            myTransaction = transaction;
            myExecutionPahse = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getTransactionID()
        {
            return myTransaction.getTransactionID();
        }
        
        /**
         * Returns the {@link ClientInfo} to which response is to be 
         * sent.
         */
        protected ClientInfo getClientInfo()
        {
            return myClientInfo;
        }
        
        /**
         * A helper method to send response to the client.
         */
        protected void sendResponseToClient(TransactionResult result)
        {
            Message message =
                result.isCommitted() ?
                    new DBOperationSuccessMessage(
                        myServerID,
                        myClientInfo.getClientSequenceNumber(), 
                        result.getResult())
                    : new DBOperationFailureMessage(
                          myServerID,
                          myClientInfo.getClientSequenceNumber(),
                          "Failed to apply the transaction on db");

            myEventBus.publish(
               ActorID.DB_ENGINE,
               new SendMessageEvent(
                   Collections.singleton(myClientInfo.getClientID()),
                   message));
        }

        /**
         * Sends error notification to the client.
         */
        protected void sendErrorToClient(Exception exception)
        {
            if (myClientInfo != null) {
                myEventBus.publish(
                   ActorID.DB_ENGINE,
                   new SendMessageEvent(
                       Collections.singleton(
                           myClientInfo.getClientID()),
                       new DBOperationFailureMessage(
                           myServerID,
                           myClientInfo.getClientSequenceNumber(),
                           exception.getMessage(),
                           exception)));
            }
        }
        
        /**
         * {@inheritDoc}
         */
        public void initiateCommit()
        {
            if (myMemento != null) {
                ListenableFuture<Memento<TransactionResult>> future =
                    myExecutor.submit(
                       new PhasedTransactionExecutor<TransactionResult>(
                           myMemento));
        
                Futures.addCallback(future,
                                    new WorkflowProcessor<>(
                                        SimpleWorkflow.this));
            }
        }
        
        /**
         * {@inheritDoc}
         */
        public void start()
        {
            if (myExecutionPahse) {
                ListenableFuture<Memento<Boolean>> future =
                    myExecutor.submit(
                       new PhasedTransactionExecutor<Boolean>(
                           myTransaction,
                           new PhasedTransactionExecutor.ExecutionPhase(
                               myTransaction)));
                    
                Futures.addCallback(future, 
                                    new WorkflowProcessor<>(SimpleWorkflow.this));

            }
            else {
                LOG.severe("For " + getTransactionID() + " we are initiating "
                           + " start when commit is expected ");
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void respondTO(Object event)
        {
            if (event instanceof Memento && myExecutionPahse) {
                
                @SuppressWarnings("unchecked")
                Memento<Boolean> result = (Memento<Boolean>) event;
                if (result.getPhase().getResult()) {
                    myMemento = result;
                    TLongSet precedentTransactions = 
                        Registry.getPrecedencyFor(getTransactionID());
                    if (   precedentTransactions == null
                        || precedentTransactions.isEmpty())
                    {
                        // We can commit the changes
                        initiateCommit();
                    }
                }
                else {
                    sendErrorToClient(new DatabaseException(
                        "Transaction validation failed"));
                }
                myExecutionPahse = false;
            }
            else if (event instanceof Memento && !myExecutionPahse) {
                
                myEventBus.publish(
                    ActorID.DB_ENGINE, myDatabase.getStatistics());
                
                if (myTransaction instanceof WriteTransaction) {
                    myEventBus.publish(
                        ActorID.DB_ENGINE,
                        new ReplicationProposal(
                            myReplicationUnitID,
                            ((WriteTransaction) myTransaction).getMutation(),
                            myTransaction.getStartTime(),
                            myTransaction.getEndTime()));
                }
                
                @SuppressWarnings("unchecked")
                Memento<TransactionResult> result = 
                    (Memento<TransactionResult>) event;
                sendResponseToClient(result.getPhase().getResult());
                scheduleNextTransactions(myTransaction.getTransactionID());
            }
            else if (event instanceof Exception) {
                Exception exception = (Exception) event;
                sendErrorToClient(exception);
                // XXX Abort all transactions that is dependent 
                // on this transaction.
            }
        }
    }
    
    private class DeletionWorkflow extends SimpleWorkflow
    {
        private final DeleteRangeMutation myMutation;

        /**
         * CTOR
         */
        public DeletionWorkflow(
            ClientInfo clientInfo,
            AbstractTransaction transaction,
            DeleteRangeMutation mutation)
        {
            super(clientInfo, transaction);
            myMutation = mutation;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        protected void sendResponseToClient(TransactionResult result)
        {
            myEventBus.publish(
                ActorID.DB_ENGINE,
                new SendMessageEvent(
                    Collections.singletonList(getClientInfo().getClientID()),
                    new DataLoadResponse(myServerID,
                                         myMutation.getTableName(), 
                                         MutationWrapper.wrapRangeMutation(
                                             myMutation.getDeletedData()
                                                       .get(0)
                                                       .getClass(),
                                             myMutation.getDeletedData()))));
        }
    }
    
    private class DistributedWorkflow extends AbstractWokflow
    {
        private final AbstractTransaction myTransaction;
        
        private final ClientInfo myClientInfo;
        
        private final ProposalNotificationEvent myPne;
        
        private boolean myExecutionPhase;
        
        private Memento<Boolean> myMemento;

        /**
         * CTOR
         */
        public DistributedWorkflow(AbstractTransaction transaction,
                                   ClientInfo clientInfo,
                                   ProposalNotificationEvent pne)
        {
            super();
            myTransaction = transaction;
            myClientInfo = clientInfo;
            myPne = pne;
            myExecutionPhase = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public long getTransactionID()
        {
            return myTransaction.getTransactionID();
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void start()
        {
            if (myDatabase.canProcess(getTransactionID())) {
                // Now schedule a distributed transaction to 
                // execute.
                ListenableFuture<Memento<Boolean>> future =
                    myExecutor.submit(
                       new PhasedTransactionExecutor<Boolean>(
                           myTransaction,
                           new PhasedTransactionExecutor.ExecutionPhase(
                               myTransaction)));
                    
                Futures.addCallback(future, 
                                    new WorkflowProcessor<>(
                                        DistributedWorkflow.this));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void respondTO(Object event)
        {
            if (event instanceof Memento && myExecutionPhase) {
                @SuppressWarnings("unchecked")
                Memento<Boolean> result = (Memento<Boolean>) event;
                if (myPne != null) {
                    myEventBus.publish(
                        ActorID.DB_ENGINE,
                        new ProposalNotificationResponse(
                            myPne,
                            result.getPhase().getResult()));
                }
                myMemento = result;
            }
            else if (event instanceof Memento && myExecutionPhase) {
                myEventBus.publish(
                   ActorID.DB_ENGINE,
                   new ProposalNotificationResponse(myPne, false));
            }
            else if (   event instanceof ProposalNotificationEvent
                     && ((ProposalNotificationEvent) event).isCommitNotification()
                     && myMemento != null) 
            {
                myExecutionPhase = false;
                initiateCommit();
            }
            else if ( event instanceof ConsensusResponseEvent) {
                myExecutionPhase = false;
                initiateCommit();
            }
            else if (   !myExecutionPhase 
                     && event instanceof Memento)
            {
                @SuppressWarnings("unchecked")
                Memento<TransactionResult> result = 
                    (Memento<TransactionResult>) event;
                
                // THis is wrong. Figure out a way to fix this */
                
                /* myEventBus.publish(ActorID.DB_ENGINE, 
                                      myDatabase.getStatistics()); */
                
                if (myTransaction instanceof WriteTransaction) {
                    myEventBus.publish(
                        ActorID.DB_ENGINE,
                        new ReplicationProposal(
                            myReplicationUnitID,
                            ((WriteTransaction) myTransaction).getMutation(),
                            myTransaction.getStartTime(),
                            myTransaction.getEndTime()));
                }
                if (myClientInfo != null) {
                    Message message =
                        result.getPhase().getResult().isCommitted() ?
                            new DBOperationSuccessMessage(
                                myServerID,
                                myClientInfo.getClientSequenceNumber(), 
                                result.getPhase().getResult().getResult())
                            : new DBOperationFailureMessage(
                                  myServerID,
                                  myClientInfo.getClientSequenceNumber(),
                                  "Failed to apply the transaction on db");
    
                    myEventBus.publish(
                       ActorID.DB_ENGINE,
                       new SendMessageEvent(
                           Collections.singleton(myClientInfo.getClientID()),
                           message));
                }
                
                // scheduleNextTransactions(myTransaction.getTransactionID());
                List<WorkFlow> queued =
                    new ArrayList<>(myQueuedWorkFlows);
                myQueuedWorkFlows.clear();
                myExecutor.submit(new ScheduleTransactionExecutionTask(queued));
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void initiateCommit()
        {
            if (myMemento != null) {
                ListenableFuture<Memento<TransactionResult>> future =
                    myExecutor.submit(
                       new PhasedTransactionExecutor<TransactionResult>(
                           myMemento));

                Futures.addCallback(future,
                                    new WorkflowProcessor<>(
                                        DistributedWorkflow.this));
            }
        }
    }
    
    private class WorkflowProcessor<T> implements FutureCallback<T>
    {
        private final WorkFlow myWorkFlow;

        /**
         * CTOR
         */
        public WorkflowProcessor(WorkFlow workFlow)
        {
            super();
            myWorkFlow = workFlow;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable exception)
        {
            myWorkFlow.respondTO(exception);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(T result)
        {
            myWorkFlow.respondTO(result);
        }
    }
    
    private class ScheduleTransactionExecutionTask implements Runnable
    {
        private final List<WorkFlow> myList;
        
        /**
         * CTOR
         */
        public ScheduleTransactionExecutionTask(List<WorkFlow> list)
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
            for (WorkFlow workFlow : myList) {
                workFlow.start();
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
                WorkFlow workFlow = 
                    myWorkFlowMap.get(Long.valueOf(transactionID));
                if (workFlow != null) {
                    if (workFlow instanceof SimpleWorkflow)  {
                        workFlow.initiateCommit();
                    }
                    else {
                        workFlow.start();
                    }
                }
            }
        }
    }

    /** LOGGER */
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(TransactionManager.class);
    
    private final Clock myClock;

    private final TransactableDatabase myDatabase;

    private final EventBus myEventBus;

    private final ListeningExecutorService myExecutor;

    private final IDAssigner myIdAssigner;

    private final NodeID myServerID;

    private final List<WorkFlow> myQueuedWorkFlows; 
    
    private final Map<Long, WorkFlow> myWorkFlowMap;
    
    private final Map<UnitID, WorkFlow> myConsensusToWorkFlowMap;
    
    private final UnitID myReplicationUnitID;
    
    private EngineJanitor myJanitor;
    
    /**
     * CTOR
     */
    @Inject
    public TransactionManager(TransactableDatabase database,
                              Clock                clock,
                              EventBus             eventBus,
                              NodeID               serverID,
                              UnitID               replicationID)
    {
        myDatabase = database;
        myClock = clock;
        myIdAssigner = new IDAssigner();
        myServerID = serverID;
        myEventBus = eventBus;
        myQueuedWorkFlows = new CopyOnWriteArrayList<>();
        myWorkFlowMap = new ConcurrentHashMap<>();
        myConsensusToWorkFlowMap = new ConcurrentHashMap<>();
        myReplicationUnitID = replicationID;
        myExecutor =
            MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(
                    20,
                    new NamedThreadFactory(TransactionManager.class)));
    }
    
    /** 
     * Initializes reference to the Janitor that's responsible for this 
     * TransactionManager.
     */
    public void initialize(EngineJanitor janitor)
    {
        myJanitor = janitor;
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
    public void processQueryAndDeleteOperation(
        NodeID originatorNode, String tableName, Range<?> deletedRange)
    {
        long id = myIdAssigner.getTransactionID();
        DeleteRangeMutation operation = 
            new DeleteRangeMutation(tableName, deletedRange);

        AbstractTransaction transaction =
            new WriteTransaction(
                id, myDatabase, myClock, operation);
        
        ClientInfo clientInfo = new ClientInfo(originatorNode, -1L);
        WorkFlow workFlow = 
            new DeletionWorkflow(clientInfo, transaction, operation);
        myWorkFlowMap.put(Long.valueOf(id), workFlow);
        
        if (myDatabase.lock(id)) {
            // The new distributed transaction will be 
            // dependent on all other transactions to complete.
            Registry.addDependencyToAll(id);
        }
    }
    
    /**
     * Creates appropriate <code>Transaction<code> to process the mutations/
     * queries on the database.
     */
    public void processOperation(Mutation mutation)
    {
        processOperation(null, mutation, -1);
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
        
        AbstractTransaction transaction =
            operation instanceof Mutation ?
                new WriteTransaction(
                    id, myDatabase, myClock, (Mutation) operation)
                : new ReadTransaction(
                    id, myDatabase, myClock, (Query) operation);
        
                    
        WorkFlow workFlow = 
            new SimpleWorkflow(clientInfo, transaction);
        
        myWorkFlowMap.put(Long.valueOf(id), workFlow);
        
        if (myDatabase.canProcess(id)) {
            ListenableFuture<Memento<Boolean>> future =
                myExecutor.submit(
                   new PhasedTransactionExecutor<Boolean>(
                       transaction,
                       new PhasedTransactionExecutor.ExecutionPhase(transaction)));
    
            Futures.addCallback(future, 
                                new WorkflowProcessor<>(workFlow));
        }
        else {
            myQueuedWorkFlows.add(workFlow);
        }
    }

    /**
     * Creates appropriate <code>Transaction<code>s to process the mutations/
     * queries to be performed on multitude of nodes of the database.
     */
    public void processOperation(NodeID                   clientID,
                                 long                     sequenceNumber,
                                 Map<NodeID, DBOperation> operations)
    {
        Set<NodeID> acceptors =
            Sets.difference(operations.keySet(),
                            Collections.singleton(myServerID));
        UnitID unitID = new DistributedTrnID(clientID, sequenceNumber);
        myEventBus.publish(
            ActorID.DB_ENGINE,
            new CreateConsensusLeaderEvent(unitID, acceptors));

        long id = myIdAssigner.getTransactionID();
        DBOperation operation = operations.get(myServerID);

        AbstractTransaction transaction =
            operation instanceof Mutation ?
                new WriteTransaction(
                    id, myDatabase, myClock, (Mutation) operation)
                : new ReadTransaction(
                    id, myDatabase, myClock, (Query) operation);
        
        ClientInfo clientInfo = new ClientInfo(clientID, sequenceNumber);
        WorkFlow workFlow = 
            new DistributedWorkflow(transaction, clientInfo, null);
        myWorkFlowMap.put(Long.valueOf(id), workFlow);
        myConsensusToWorkFlowMap.put(unitID, workFlow);
        
        myEventBus.publish(
            ActorID.DB_ENGINE,
            new ConsensusRequestEvent(
                new DistributedTrnProposal(
                    new DistributedTrnID(clientID, sequenceNumber),
                    operations,
                    id)));
        
        if (myDatabase.lock(id)) {
            // The new distributed transaction will be 
            // dependent on all other transactions to complete.
            Registry.addDependencyToAll(id);
        }
    }
    
    public void processOperation(ConsensusResponseEvent response)
    {
        WorkFlow workflow = 
            myConsensusToWorkFlowMap.get(response.getProposal().getUnitID());
        workflow.respondTO(response);
    }

    /**
     * Creates appropriate <code>Transaction<code>s to process the mutations/
     * queries to be performed on multitude of nodes of the database.
     */
    public void processOperation(ProposalNotificationEvent pne)
    {
        if (pne.getProposal() instanceof ReplicationProposal) {
            ReplicationProposal replicationProposal = 
                (ReplicationProposal) pne.getProposal();
            long id = myIdAssigner.getTransactionID();
            ReplicatedWriteTransaction transaction =
                new ReplicatedWriteTransaction(
                    id, 
                    myDatabase,
                    myClock,
                    replicationProposal.getMutation(),
                    replicationProposal.getStart(),
                    replicationProposal.getEndTime());
            myExecutor.submit(new ReplicationExecutor(transaction));
        }
        else if (pne.isCommitNotification()) {
           WorkFlow workFlow = 
               myConsensusToWorkFlowMap.get(pne.getProposal().getUnitID());
           if (workFlow != null) {
               workFlow.initiateCommit();
           }
       }
       else {        
           DistributedTrnProposal distributedTrnProposal =
               (DistributedTrnProposal) pne.getProposal();

           DBOperation operation =
               distributedTrnProposal.getNodeToDBOperationMap()
                                     .get(myServerID);

           long id = myIdAssigner.getTransactionID();
           AbstractTransaction transaction =
               operation instanceof Mutation ?
                   new WriteTransaction(
                       id, myDatabase, myClock, (Mutation) operation)
                   : new ReadTransaction(
                       id, myDatabase, myClock, (Query) operation);
                       
           WorkFlow workFlow = 
               new DistributedWorkflow(transaction, null, pne);
           myWorkFlowMap.put(Long.valueOf(id), workFlow);
           myConsensusToWorkFlowMap.put(pne.getProposal().getUnitID(), 
                                        workFlow);

           if (myDatabase.lock(id)) {
               // The new distributed transaction will be 
               // dependent on all other transactions to complete.
               Registry.addDependencyToAll(id);
           }
       }
    }
}
