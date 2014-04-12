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

import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * A simple type that captures the result of validating a <code>Transactable
 * </code> with the given id.
 * 
 * @author Balraja Subbiah
 */
public class ValidationResult implements Poolable
{
    private boolean myIsValid;
    
    private boolean myIsSpeculativelyValid;
    
    private long myTransactionId;

    /**
     * Factory method for creating an instance of <code>ValidationResult</code> 
     * and populating with various parameters.
     */
    public static ValidationResult create(boolean isValid, long transactionID)
    {
        ValidationResult result = PooledObjects.getInstance(ValidationResult.class);
        result.myIsValid = isValid;
        result.myIsSpeculativelyValid = false;
        result.myTransactionId = transactionID;
        return result;
    }
    

    /**
     * Returns the value of isValid
     */
    public boolean isValid()
    {
        return myIsValid;
    }

    /**
     * Returns the value of isSpeculativelyValid
     */
    public boolean isSpeculativelyValid()
    {
        return myIsSpeculativelyValid;
    }

    /**
     * Returns the value of transactionId
     */
    public long getTransactionId()
    {
        return myTransactionId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myIsValid = myIsSpeculativelyValid = false;
        myTransactionId = Long.MIN_VALUE;
    }
}
