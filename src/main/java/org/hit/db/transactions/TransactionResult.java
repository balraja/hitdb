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

/**
 * The type for capturing the result of a <code>Transaction</code>
 * 
 * @author Balraja Subbiah
 */
public class TransactionResult
{
    private final long myTransactionID;
    
    private final boolean myCommitted;
    
    private final Object myResult;
    

    /**
     * CTOR
     */
    public TransactionResult(long transactionID, boolean committed)
    {
        this(transactionID, committed, null);
    }

    /**
     * CTOR
     */
    public TransactionResult(long transactionID,
                             boolean committed,
                             Object result)
    {
        super();
        myTransactionID = transactionID;
        myCommitted = committed;
        myResult = result;
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
}
