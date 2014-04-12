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

import org.hit.pool.Poolable;

/**
 * Defines the contract for a transaction that reads or modifies values in a
 * table.
 * 
 * @author Balraja Subbiah
 */
public interface Transaction extends Poolable
{
    /** Defines a null value for transaction time */
    public static final long CONST_NULL_TIME = 0L;
    
    /** Aborts the transaction */
    void abort();
    
    /** Commits the transaction */
    void commit();

    /** Executes the transaction on the database */
    void execute();

    /** Returns the end time of the transaction */
    long getEndTime();

    /** Returns the current state of the transaction */
    public TransactionState getMyState();
    
    /** Returns the start time of the transaction */
    long getStartTime();
    
    /** Initializes the transaction */
    void init();
    
    /** Validates the transaction */
    boolean validate();
}
