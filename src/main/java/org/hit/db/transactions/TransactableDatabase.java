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

import org.hit.db.model.Persistable;
import org.hit.db.model.Schema;
import org.hit.event.DBStatEvent;

/**
 * Wraps <code>Database</code> to keep track of the keys accessed by a
 * <code>Transaction</code> during it's lifetime.
 * 
 * @author Balraja Subbiah
 */
public interface TransactableDatabase
{
    public static final long UNLOCKED_VALUE = Long.MIN_VALUE;
    
    /** Creates a table with the specified schema in the database */
    public void createTable(Schema schema);

    /** Returns the <code>TransactableTable</code> from the database. */
    public <K extends Comparable<K>, P extends Persistable<K>> TransactableTable<K, P>
        lookUpTable(String tableName);
    
    /**
     * Generates the statistics for database.
     */
    public DBStatEvent getStatistics();
    
    /**
     * Locks the database. By locking the database a transaction will 
     * wait for all other transactions to complete, and once locked 
     * will not allow any other transaction till the transaction
     * that locked the database is done.
     * 
     * @return True, if we can lock the db successfully, false otherwise.
     */
    public boolean lock(long transactionID);
    
    /**
     * Unlocks the database to allow other transactions to continue
     * their work.
     * 
     * @return True, if we can unlock the db successfully, false otherwise.
     */
    public boolean unlock(long transactionID);
    
    /**
     * Returns true if the database can process a transaction, false 
     * otherwise. Database will allow transactions to execute
     * if it has been not locked by a transaction. Else it will allow
     * only that transaction that locked the database to continue.
     */
    public boolean canProcess(long transactionID);
    
    /**
     * Returns the transaction that has locked the database.
     */
    public long getLockedTransaction();
}
