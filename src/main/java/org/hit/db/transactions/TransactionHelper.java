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

/**
 * A helper class to assist in performing transactions.
 * 
 * @author Balraja Subbiah
 */
public final class TransactionHelper
{
    /** Defines the contract for an infinite time */
    public static final long INFINITY = Long.MAX_VALUE;
    
    /**
     * Returns true if the given version number corresponding to a transaction.
     */
    public static boolean isTransactionID(long version)
    {
        return version < 0;
    }
    
    /**
     * Returns the actual id corresponding to a transaction from an object's
     * version number
     */
    public static long toTransactionID(long version)
    {
        return Math.abs(version);
    }

    /**
     * Returns the version number corresponding to a transaction to be stored
     * as an object's version.
     */
    public static long toVersionID(long transactionID)
    {
        return -Math.abs(transactionID);
    }

    /** Private CTOR to avoid initialization */
    private TransactionHelper()
    {
    }
}
