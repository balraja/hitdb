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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Type that can be used for assigning unique identifiers to the
 * transactions.
 * 
 * @author Balraja Subbiah
 */
public class IDAssigner
{
    private final AtomicLong myTransactionID;

    /**
     * CTOR
     */
    public IDAssigner()
    {
        myTransactionID = new AtomicLong(1L);
    }

    /** Returns the latest unique transaction identifier for a transaction */
    public long getTransactionID()
    {
        return myTransactionID.getAndIncrement();
    }
}
