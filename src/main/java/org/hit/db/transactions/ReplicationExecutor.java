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
 * Implements a callable to perform replicated actions on a database.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(size=1000,initialSize=100)
public class ReplicationExecutor implements Runnable, Poolable
{
    private WriteTransaction myReplicatedWriteTransaction;
    
    /**
     * CTOR
     */
    public static ReplicationExecutor create(
        WriteTransaction replicatedWriteTransaction,
        long start,
        long end)
    {
        ReplicationExecutor executor =
            PooledObjects.getInstance(ReplicationExecutor.class);
        executor.myReplicatedWriteTransaction = replicatedWriteTransaction;
        executor.myReplicatedWriteTransaction.setTimeOverride(start, end);
        return executor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run()
    {
        myReplicatedWriteTransaction.init();
        myReplicatedWriteTransaction.execute();
        myReplicatedWriteTransaction.commit();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        PooledObjects.freeInstance(myReplicatedWriteTransaction);
        myReplicatedWriteTransaction = null;
    }
    
}
