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

package org.hit.db.transactions;

import java.util.Collection;
import java.util.Collections;

import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.HitTableSchema;
import org.hit.db.model.Table;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Defines the contract for adaptor that adapts {@link Table} to read/write data
 * from {@link TransactableTable}. In that process it keeps track of the version
 * numbers of the objects read/written by the {@link Transaction}.
 *
 * @author Balraja Subbiah
 */
public class TableAdaptor<K extends Comparable<K>, P extends Persistable<K>>
    implements Table<K, P>
{
    /**
     * Implements {@link Function} to transform <code>Transactable</code>
     * </code>Persistable</code>.
     */
    private static class Transactable2Persistable<K extends Comparable<K>,
                                                  P extends Persistable<K>>
        implements Function<Transactable<K, P>, P>
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public P apply(Transactable<K, P> transactable)
        {
            return transactable.getPersistable();
        }
    }
    
    private final long                        myStartTime;

    private final TransactableTable<K, P>     myTable;

    private final TransactionTableTrail<K, P> myTableTrail;

    private final long                        myTransactionID;
    
    /**
     * CTOR
     */
    public TableAdaptor(TransactableTable<K, P> table,
                        long startTime,
                        long transactionID)
    {
        myTable      = table;
        myStartTime  = startTime;
        myTableTrail =
            new TransactionTableTrail<>(myTable.getSchema().getTableName());
        myTransactionID = transactionID;
    }

    /**
     * A helper method to commit the updates made by the transaction.
     *
     * @param commitTime The time to which end time of a record is set to.
     */
    public void commit(long commitTime)
    {
        // Close the old version.
        for (Transactable<K,P> transactable : myTableTrail.getWriteSet()) {
            transactable.setEnd(commitTime);
        }
        // Update the start time of the new version to the commit time.
        for (Transactable<K,P> transactable : myTableTrail.getNewWriteSet()) {
            transactable.setStart(commitTime);
        }
    }
    
    /**
     * A helper method to undo the updates made by the transaction.
     *
     * @param commitTime The time to which end time of a record is set to.
     */
    public void abort()
    {
        // Close the old version.
        for (Transactable<K,P> transactable : myTableTrail.getWriteSet()) {
            if (   TransactionHelper.isTransactionID(transactable.getEnd())
                && TransactionHelper.toTransactionID(transactable.getEnd()) == 
                       myTransactionID)
            {
                transactable.setEnd(TransactionHelper.INFINITY);
            }
        }
        
        // Delete the new versions added to the database.
        for (Transactable<K,P> transactable : myTableTrail.getNewWriteSet()) {
            myTable.remove(transactable);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> findMatching(Predicate predicate)
    {
        Collection<Transactable<K,P>> result =
            myTable.findMatching(predicate, myStartTime, myTransactionID);
        myTableTrail.getPredicateToDataMap().put(
            new PredicateWrapper<K,P>(predicate), result);
        Collection<P> actualResult =
            Collections2.transform(result, new Transactable2Persistable<K,P>());
        return Collections.unmodifiableCollection(actualResult);
    }
   
    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> findMatching(Predicate  predicate,  
                                      K          start, 
                                      K          end)
    {
        Collection<Transactable<K,P>> result =
            myTable.findMatching(
                predicate, start, end, myStartTime, myTransactionID);
        myTableTrail.getPredicateToDataMap().put(
           new PredicateWrapper<K,P>(predicate), result);
        Collection<P> actualResult =
            Collections2.transform(result, new Transactable2Persistable<K,P>());
        return Collections.unmodifiableCollection(actualResult);    
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P getRow(K primarykey)
    {
        
        Transactable<K, P> result =
            myTable.getRow(primarykey, myStartTime, myTransactionID);
        
        if (result != null) {
            myTableTrail.getReadSet().add(result);
            return result.getPersistable();
        }
        else {
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HitTableSchema getSchema()
    {
        return myTable.getSchema();
    }

    /**
     * Returns the value of startTime
     */
    public long getStartTime()
    {
        return myStartTime;
    }

    /**
     * Returns the value of tableTrail
     */
    public TransactionTableTrail<K, P> getTableTrail()
    {
        return myTableTrail;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean update(P old, P updated)
    {
        if (old != null) {
            if (!old.primaryKey().equals(updated.primaryKey())) {
                return false;
            }
            Transactable<K, P> tableOld =
                 myTable.getRow(updated.primaryKey(), myStartTime, myTransactionID);

            
            if (!tableOld.getPersistable().equals(old)) {
                return false;
            }
            
            ValidationResult result = 
                tableOld.validate(myStartTime, myTransactionID);
                
            if (!result.isValid() && !result.isSpeculativelyValid()) {
                return false;
            }
            
            if (result.isSpeculativelyValid()) {
                Registry.addDependency(result.getTransactionId(), 
                                       myTransactionID);
            }
            
            // Now lock the row for this transaction
            tableOld.setEnd(TransactionHelper.toVersionID(myTransactionID));
            myTableTrail.getWriteSet().add(tableOld);
        }

        Transactable<K,P> updatedTransactable =
            new Transactable<K, P>(updated);
        updatedTransactable.setStart(
            TransactionHelper.toVersionID(myTransactionID));
        updatedTransactable.setEnd(TransactionHelper.INFINITY);

        myTableTrail.getNewWriteSet().add(updatedTransactable);
        myTable.addToTable(updatedTransactable);
        return true;
    }

    /**
     * Returns true if the <code>TransactionTableTrail</code> is valid.
     */
    public boolean validate(TransactionValidator validator)
    {
        return validator.isAcceptable(myTableTrail);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P deleteRow(K primaryKey)
    {
        Transactable<K, P> result = myTable.deleteRow(primaryKey, 
                                                      myStartTime,
                                                      myTransactionID);
                                                      
        return result != null ? result.getPersistable() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> deleteRange(K primaryKey, K secondaryKey)
    {
        Collection<Transactable<K,P>> result =
            myTable.deleteRange(primaryKey, 
                                secondaryKey, 
                                myStartTime, 
                                myTransactionID);
        
        // XXX Not sure about how to validate
        /*
        myTableTrail.getPredicateToDataMap().put(
           new PredicateWrapper<K,P>(predicate), result);  */
        
        Collection<P> actualResult =
            Collections2.transform(result, new Transactable2Persistable<K,P>());
        return Collections.unmodifiableCollection(actualResult);    
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Collection<P> deleteRange(Object primaryKey, Object secondaryKey)
    {
        return deleteRange((K) primaryKey, (K) secondaryKey);
    }
}
