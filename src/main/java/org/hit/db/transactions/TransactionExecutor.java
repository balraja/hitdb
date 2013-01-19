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

import java.util.concurrent.Callable;

import org.hit.db.transactions.journal.WAL;

/**
 * The type that is responsible for executing <code>Transaction</code>s.
 * 
 * @author Balraja Subbiah
 */
public class TransactionExecutor implements Callable<TransactionResult>
{
    private final AbstractTransaction myTransaction;
    
    private final WAL myWriteAheadLog;
    
    /**
     * CTOR
     */
    public TransactionExecutor(AbstractTransaction transaction,
                               WAL                 writeAheadLog)
    {
        myTransaction = transaction;
        myWriteAheadLog = writeAheadLog;
    }
    
    /**
     * Executes the transactions.
     */
    @Override
    public TransactionResult call()
    {
        myTransaction.execute();
        if (myTransaction.validate()) {
            if (myTransaction instanceof WriteTransaction) {
                myWriteAheadLog.addTransaction((WriteTransaction) myTransaction);
            }
            myTransaction.commit();
        }
        else {
            myTransaction.abort();
        }
        
        if (myTransaction instanceof ReadTransaction) {
            return new TransactionResult(myTransaction.getTransactionID(),
                                         myTransaction.getMyState()
                                             == TransactionState.COMMITTED,
                                         ((ReadTransaction) myTransaction)
                                             .getResult());
        }
        else {
            return new TransactionResult(myTransaction.getTransactionID(),
                                         myTransaction.getMyState()
                                             == TransactionState.COMMITTED);
        }
    }
}
