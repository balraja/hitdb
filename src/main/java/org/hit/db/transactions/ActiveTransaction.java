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

import org.hit.time.Clock;

/**
 * Defines the contract for a transaction that' performed actively
 * on a database.
 * 
 * @author Balraja Subbiah
 */
public abstract class ActiveTransaction extends AbstractTransaction
{
    private Clock myClock;

    /**
     * CTOR
     */
    public static void initialize(
        ActiveTransaction transaction,
        long transactionId,
        TransactableDatabase database,
        boolean updateRegistry,
        Clock clock)
    {
        AbstractTransaction.initialize(
            transaction, transactionId, database, updateRegistry);
        transaction.myClock = clock;
    }
    
    @Override
    public void free()
    {
        super.free();
        myClock = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long makeStartTime()
    {
        return myClock.currentTime();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected long makeEndTime()
    {
        return myClock.currentTime();
    }
}
