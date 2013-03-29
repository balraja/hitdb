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

import org.hit.db.transactions.PhasedTransactionExecutor.Phase;
import org.hit.db.transactions.journal.WAL;

/** 
 * The class that stores the intermediate state between phases 
 */
public class Memento<T>
{
    private final AbstractTransaction myTransaction;
    
    private final WAL myWriteAheadLog;
    
    private final Phase<T> myPhase;
    
    /**
     * CTOR
     */
    public Memento(AbstractTransaction transaction, WAL writeAheadLog,
                   Phase<T> phase)
    {
        super();
        myTransaction = transaction;
        myWriteAheadLog = writeAheadLog;
        myPhase = phase;
    }

    /**
     * Returns the value of transaction
     */
    public AbstractTransaction getTransaction()
    {
        return myTransaction;
    }

    /**
     * Returns the value of writeAheadLog
     */
    public WAL getWriteAheadLog()
    {
        return myWriteAheadLog;
    }

    /**
     * Returns the value of phase
     */
    public PhasedTransactionExecutor.Phase<T> getPhase()
    {
        return myPhase;
    }
}