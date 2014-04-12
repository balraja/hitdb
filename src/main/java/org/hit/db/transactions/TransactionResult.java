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

import org.hit.pool.PoolConfiguration;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * The type for capturing the result of a <code>Transaction</code>
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(size=20000,initialSize=200)
public class TransactionResult implements Poolable
{
    private long myTransactionID;
    
    private boolean myCommitted;
    
    private Object myResult;
    
    /**
     * Factory method for creating an instance of <code>TransactionResult</code> 
     * and populating with various parameters.
     */
    public static TransactionResult create(
        long transactionID, boolean committed, Object executionResult)
    {
        TransactionResult result = 
            PooledObjects.getInstance(TransactionResult.class);
        result.myTransactionID = transactionID;
        result.myCommitted = committed;
        result.myResult = executionResult;
        return result;
    }

    /**
     * Returns the value of result
     */
    public Object getResult()
    {
        return myResult;
    }

    /**
     * Returns the value of transactionID
     */
    public long getTransactionID()
    {
        return myTransactionID;
    }

    /**
     * Returns true if the transaction has produced some result.
     */
    public boolean hasResult()
    {
        return myResult != null;
    }
    
    /**
     * Returns the value of committed
     */
    public boolean isCommitted()
    {
        return myCommitted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myTransactionID = Long.MIN_VALUE;
        myCommitted = false;
        myResult = null;
    }
}
