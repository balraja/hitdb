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

package org.hit.db.transactions;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;
import org.hit.util.LogFactory;

/**
 * Defines the contract for the interface that perfoms two phased execution 
 * of transactions. In the first phase, it executes the transaction and 
 * validates that whether the transaction can be committed.
 * 
 * @author Balraja Subbiah
 */
public class PhasedTransactionExecutor<T> 
    implements Callable<Memento<T>>,Poolable
{
    /** LOGGER */
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(PhasedTransactionExecutor.class);
        
    /**
     * Defines the contract for the phase of transaction execution.
     */
    public static interface Phase<P> extends Poolable
    {
        /** Executes the current state */
        public void execute();
        
        /**
         * Returns the response for this execution phase.
         */
        public P getResult();
        
        /** Returns the next phase to be executed after this phase */
        public Phase<?> nextPhase(Memento<?> memento);
    }
    
    public static class ExecutionPhase implements Phase<Boolean>
    {
        private Boolean myResult;
        
        private AbstractTransaction myTransaction;

        /**
         * Factory method for creating an instance of 
         * <code>PhasedTransactionExecutor.ExecutionPhase</code> 
         * and populating it with various parameters.
         */
        public static ExecutionPhase create(AbstractTransaction transaction)
        {
            ExecutionPhase executionPhase = 
                PooledObjects.getInstance(ExecutionPhase.class);
            executionPhase.myTransaction = transaction;
            executionPhase.myResult = false;
            return executionPhase;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void execute()
        {
            myTransaction.init();
            myTransaction.execute();
            myResult = Boolean.valueOf(myTransaction.validate());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Boolean getResult()
        {
            return myResult;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Phase<?> nextPhase(Memento<?> memento)
        {
            return CommitPhase.create(memento.getTransaction());
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void free()
        {
            myTransaction = null;
            myResult      = false;
        }
    }
    
    private static class CommitPhase implements Phase<TransactionResult>
    {
        private TransactionResult myResult;
        
        private AbstractTransaction myTransaction;
        
        /**
         * Factory method for creating an instance of 
         * <code>PhasedTransactionExecutor.CommitPhase</code> 
         * and populating with various parameters.
         */
        public static CommitPhase create(AbstractTransaction transaction)
        {
            CommitPhase commitPhase = 
                PooledObjects.getInstance(CommitPhase.class);
            commitPhase.myTransaction = transaction;
            commitPhase.myResult = null;
            return commitPhase;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void execute()
        {
            if (myTransaction.validate()) {
                myTransaction.commit();
            }
            else {
                myTransaction.abort();
            }
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("The transaction " + myTransaction.getTransactionID()
                        + " of type " + myTransaction.getClass().getSimpleName()
                         + " is in state " + myTransaction.getMyState()); 
            }
            
            if (myTransaction instanceof ReadTransaction) {
                myResult =
                    TransactionResult.create(myTransaction.getTransactionID(),
                                             myTransaction.getMyState()
                                                 == TransactionState.COMMITTED,
                                             ((ReadTransaction) myTransaction)
                                                 .getResult());
            }
            else {
                myResult = TransactionResult.create(
                                myTransaction.getTransactionID(),
                                myTransaction.getMyState()
                                    == TransactionState.COMMITTED,
                                null);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TransactionResult getResult()
        {
            return myResult;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Phase<?> nextPhase(Memento<?> memento)
        {
            return null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void free()
        {
            myTransaction = null;
            PooledObjects.freeInstance(myResult);
            myResult = null;
        }
    }
    
    private AbstractTransaction myTransaction;
    
    private Phase<T> myPhase;
    
    /**
     * Factory method for creating an instance of 
     * <code>PhasedTransactionExecutor</code> 
     * and populating with various parameters.
     */
    public static <T> PhasedTransactionExecutor<T> create(
          AbstractTransaction transaction,
          Phase<T>            phase)
    {
        @SuppressWarnings("unchecked")
        PhasedTransactionExecutor<T> phasedExecutor = 
            PooledObjects.getInstance(PhasedTransactionExecutor.class);
        phasedExecutor.myTransaction = transaction;
        phasedExecutor.myPhase = phase;
        return phasedExecutor;
    }
    
    /**
     * Factory method for creating an instance of 
     * <code>PhasedTransactionExecutor</code> 
     * and populating with various parameters.
     */
    @SuppressWarnings("unchecked")
    public static <T> PhasedTransactionExecutor<T> create(Memento<?> memento)
    {
        PhasedTransactionExecutor<T> phasedExecutor = 
            PooledObjects.getInstance(PhasedTransactionExecutor.class);

        phasedExecutor.myTransaction = memento.getTransaction();
        phasedExecutor.myPhase = 
            (Phase<T>) memento.getPhase().nextPhase(memento);
        return phasedExecutor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Memento<T> call() throws Exception
    {
        if (myPhase != null) {
            myPhase.execute();
            return Memento.<T>create(myTransaction, myPhase);
        }
        else {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("The next phase of transaction is not found");
            }
            throw new NullPointerException(
                 "The phase for this execution is not defined");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        PooledObjects.freeInstance(myPhase);
        myPhase = null;
        myTransaction = null;
    }
}
