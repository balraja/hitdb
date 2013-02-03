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
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Mutation;
import org.hit.db.model.Query;
import org.hit.db.model.Schema;
import org.hit.db.transactions.AbstractTransaction;
import org.hit.db.transactions.IDAssigner;
import org.hit.db.transactions.ReadTransaction;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.TransactionExecutor;
import org.hit.db.transactions.TransactionID;
import org.hit.db.transactions.TransactionResult;
import org.hit.db.transactions.WriteTransaction;
import org.hit.db.transactions.journal.WAL;
import org.hit.event.ConsensusRequestEvent;
import org.hit.event.DBOperationFailureMessage;
import org.hit.event.DBOperationSuccessMessage;
import org.hit.event.SendMessageEvent;
import org.hit.time.Clock;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;
import org.hit.zookeeper.ZooKeeperClient;

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
     * The task that's executed on completion of a <code>Transaction</code>
     */
    private static class NotifyResultTask
        implements FutureCallback<TransactionResult>
    {
        private final NodeID myClientID;
        
        private final NodeID myServerID;
        
        private final EventBus myEventBus;
        
        private final TransactionID myTransactionID;
        
        private final DBOperation myOperation;

        /**
         * CTOR
         */
        public NotifyResultTask(NodeID        clientID,
                                NodeID        serverID,
                                EventBus      eventBus,
                                TransactionID transactionID,
                                DBOperation   operation)
        {
            super();
            myClientID = clientID;
            myServerID = serverID;
            myEventBus = eventBus;
            myTransactionID = transactionID;
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
                if (result.isCommitted() && myOperation instanceof Mutation) {
                    myEventBus.publish(
                        new ConsensusRequestEvent(
                            new DBReplicaProposal(
                                myTransactionID.getIdentifier(),
                                (Mutation) myOperation,
                                new ReplicatedDatabaseID(myServerID))));
                }
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
    
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(TransactionManager.class);
    
    private final ListeningExecutorService myExecutor;
    
    private final IDAssigner myIdAssigner;
    
    private final TransactableDatabase myDatabase;
    
    private final Clock myClock;
    
    private final EventBus myEventBus;
    
    private final NodeID myServerID;
    
    private final WAL    myWriteAheadLog;
    
    private final ZooKeeperClient myZooKeeperClient;
    
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
        myExecutor =
            MoreExecutors.listeningDecorator(
                Executors.newFixedThreadPool(
                    20,
                    new NamedThreadFactory(TransactionManager.class)));
    }
    
    /**
     * Creates a table with the given <code>Schema</code>
     */
    public void createTable(Schema schema)
    {
        myDatabase.createTable(schema);
        myZooKeeperClient.addKeyMeta(schema);
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
                            new NotifyResultTask(clientID,
                                                 myServerID,
                                                 myEventBus,
                                                 id,
                                                 operation));
    }
}
