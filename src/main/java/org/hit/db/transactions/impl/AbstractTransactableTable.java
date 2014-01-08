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

package org.hit.db.transactions.impl;

import java.util.List;

import org.hit.db.model.Persistable;
import org.hit.db.model.HitTableSchema;
import org.hit.db.transactions.Registry;
import org.hit.db.transactions.Transactable;
import org.hit.db.transactions.TransactableTable;
import org.hit.db.transactions.ValidationResult;
import org.hit.util.Pair;

import com.google.common.base.Function;

/**
 * Defines the contract for an abstract implementation of <code>
 * TransactableTable</code>
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractTransactableTable<K extends Comparable<K>,
                                                P extends Persistable<K>>
    implements TransactableTable<K,P>
{
    /**
     * Implements a <code>Function</code> to add dependency between 
     * transactions.
     */
    public class AddDependency
        implements Function<Pair<ValidationResult, Transactable<K, P>>, 
                            Transactable<K, P>>
    {
        private final long myTransactionID;
        
        /**
         * CTOR
         */
        public AddDependency(long transactionID)
        {
            myTransactionID = transactionID;
        }
        
        /**
         * {@inheritDoc}
         */
        public Transactable<K,P>  
            apply(Pair<ValidationResult, Transactable<K, P>> input)
        {
            if (   input.getFirst().isSpeculativelyValid()
                && input.getFirst().getTransactionId()
                       != myTransactionID)
            {
                Registry.addDependency(input.getFirst().getTransactionId(), 
                                      myTransactionID);
            }
            
            return input.getSecond();
        }
    }
    
    private final HitTableSchema mySchema;
    
    /**
     * CTOR
     */
    public AbstractTransactableTable(HitTableSchema schema)
    {
        mySchema = schema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HitTableSchema getSchema()
    {
        return mySchema;
    }

    /**
     * A helper method to get the latest version of a row for a key.
     */
    public Transactable<K, P> doGetRow(List<Transactable<K, P>> result, 
                                       long time, 
                                       long transactionID)
    {
        return doGetRow(result, time, transactionID, false);
    }
    
    /**
     * A helper method to get the latest version of a row for a key.
     */
    public Transactable<K, P> doGetRow(List<Transactable<K, P>> result, 
                                       long time, 
                                       long transactionID,
                                       boolean speculativeRead)
    {
        if (result != null) {
            for (int i = result.size() - 1; i >= 0; i--) {
                Transactable<K, P> transactable = result.get(i);
                ValidationResult validationResult = 
                    transactable.validate(time, transactionID);
                if (validationResult.isValid()) {
                    return transactable;
                }
                else if (   speculativeRead
                         && validationResult.isSpeculativelyValid()) 
                {
                    Registry.addDependency(validationResult.getTransactionId(),
                                           transactionID);
                    return transactable;
                }
            }
        }
        return null;
    }
}
