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

import org.hit.db.model.Database;
import org.hit.db.model.Mutation;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.PoolUtils;
import org.hit.pool.PooledObjects;
import org.hit.time.Clock;

/**
 * Defines the contract for the write transactions to be performed on a
 * database.
 *
 * @author Balraja Subbiah
 */
@PoolConfiguration(size=10000,initialSize=100)
public class WriteTransaction extends ActiveTransaction
{
    private Mutation myMutation;
    
    private long myStartTimeOverride;
    
    private long myEndTimeOverride;

    /**
     *   Factory method for creating an instance of <code>WriteTransaction</code> 
     * and populating with various parameters.
     */
    public static WriteTransaction create(
        long transactionId,
        TransactableDatabase database,
        Clock clock,
        Mutation mutation)
    {
        return create(transactionId, database, clock, mutation, true);
    }
    
    /**
     * CTOR
     */
    public static WriteTransaction create(
        long transactionId,
        TransactableDatabase database,
        Clock clock,
        Mutation mutation,
        boolean updateRegistry)
    {
        WriteTransaction writeTransaction =
            PooledObjects.getInstance(WriteTransaction.class);
        ActiveTransaction.initialize(
            writeTransaction, transactionId, database, updateRegistry, clock);
        writeTransaction.myMutation = mutation;
        return writeTransaction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doCommit(DatabaseAdaptor adpator)
    {
        adpator.commit(getEndTime());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doExecute(Database database)
    {
        myMutation.update(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doValidation(DatabaseAdaptor adaptor)
    {
        WriteTransactionValidator wtv =
            WriteTransactionValidator.create(adaptor.getDatabase(),
                                             getEndTime(),
                                             getTransactionID());
        return adaptor.validate(wtv);
    }

    /**
     * Returns the value of mutation
     */
    public Mutation getMutation()
    {
        return myMutation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected
        void doAbort(DatabaseAdaptor adpator)
    {
        adpator.abort();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        PoolUtils.free(myMutation);
        myMutation = null;
        myStartTimeOverride = myEndTimeOverride = Long.MIN_VALUE;
    }
    
    /** 
     * Allows overriding the start and endtimes of a transaction. Useful
     * when applying replicated transactions to the database.
     */
    public void setTimeOverride(long start, long end)
    {
        myStartTimeOverride = start;
        myEndTimeOverride   = end;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected long makeStartTime()
    {
        return myStartTimeOverride == Long.MIN_VALUE ? super.makeStartTime()
                                                     : myStartTimeOverride;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected long makeEndTime()
    {
        return myEndTimeOverride == Long.MIN_VALUE ? super.makeEndTime()
                                                   : myEndTimeOverride;
    }

}
