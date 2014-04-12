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
import java.util.Map;

import org.hit.db.model.Persistable;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.PooledObjects;

/**
 * Implements {@link TransactionValidator} for validating the reads
 * performed by a transaction.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(size=10000, initialSize=100)
public class ReadTransactionValidator implements TransactionValidator
{
    private TransactableDatabase myDatabase;
    
    private long myValidationTime;
    
    private long myTransactionId;
    
    /**
     * CTOR
     */
    public static ReadTransactionValidator create(
        TransactableDatabase database,
        long validationTime,
        long transactionId)
    {
        ReadTransactionValidator validator = 
            PooledObjects.getInstance(ReadTransactionValidator.class);
        validator.myDatabase = database;
        validator.myValidationTime = validationTime;
        validator.myTransactionId = transactionId;
        return validator;
    }
    
    /**
     * CTOR
     */
    public static void initialize(
        ReadTransactionValidator validator,
        TransactableDatabase database,
        long validationTime,
        long transactionId)
    {
        validator.myDatabase = database;
        validator.myValidationTime = validationTime;
        validator.myTransactionId = transactionId;
    }
    
    /**
     * Returns the value of database
     */
    public TransactableDatabase getDatabase()
    {
        return myDatabase;
    }

    /**
     * Returns the value of transactionId
     */
    public long getTransactionId()
    {
        return myTransactionId;
    }

    /**
     * Returns the value of validationTime
     */
    public long getValidationTime()
    {
        return myValidationTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>, P extends Persistable<K>> boolean
        isAcceptable(TransactionTableTrail<K, P> trail)
    {
        for (Transactable<K,P> transactable : trail.getReadSet()) {
            if (!transactable.validate(myValidationTime, myTransactionId)
                             .isValid()) 
            {
                return false;
            }
        }
        
        TransactableTable<K,P> table =
            myDatabase.lookUpTable(trail.getTableName());
        
        for (Map.Entry<PredicateWrapper<K>, Collection<Transactable<K,P>>>
               entry : trail.getPredicateToDataMap().entrySet())
        {
            Collection<Transactable<K,P>> newResult =
                entry.getKey().isRangeQuery()?
                    table.findMatching(
                        entry.getKey().getPredicate(),
                        entry.getKey().getStart(),
                        entry.getKey().getEnd(),
                        myValidationTime,
                        myTransactionId)
                        
                     : table.findMatching(
                           entry.getKey().getPredicate(),
                           myValidationTime,
                           myTransactionId);
            
            if (newResult.size() != entry.getValue().size()) {
                // This validation catches the phantom rows that got 
                // inserted after query is executed but before the 
                // query is validated.
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myDatabase = null;
        myValidationTime = myTransactionId = Long.MIN_VALUE;
    }
}
