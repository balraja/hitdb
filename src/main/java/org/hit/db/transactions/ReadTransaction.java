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
import org.hit.db.model.Query;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.PoolUtils;
import org.hit.pool.PooledObjects;
import org.hit.time.Clock;

/**
 * Defines the contract for a <code>Transaction</code> that performs only
 * the querying operation on a database.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(size=10000,initialSize=100)
public class ReadTransaction extends ActiveTransaction
{
    private Query myQuery;
    
    private Object myResult;

    /**
     * Factory method for creating an instance of <code>ReadTransaction</code> 
     * and populating with various parameters.
     */
    public static ReadTransaction create(
        long transactionId,
        TransactableDatabase database,
        Clock clock,
        Query query)
    {
        ReadTransaction rt = PooledObjects.getInstance(ReadTransaction.class);
        ActiveTransaction.initialize(rt, transactionId, database, true, clock);
        rt.myQuery = query;
        rt.myResult = null;
        return rt;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void doCommit(DatabaseAdaptor adpator)
    {
        // Do nothing.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doExecute(Database database)
    {
        myResult = myQuery.query(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doValidation(DatabaseAdaptor adaptor)
    {
        ReadTransactionValidator validator =
            ReadTransactionValidator.create(adaptor.getDatabase(),
                                            getEndTime(),
                                            getTransactionID());
        return adaptor.validate(validator);
    }

    /**
     * Returns the value of result
     */
    public Object getResult()
    {
        return myResult;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doAbort(DatabaseAdaptor adpator)
    {
        // Ignore as there is nothing to abort.
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        super.free();
        PoolUtils.free(myQuery);
        myQuery = null;
        PoolUtils.free(myResult);
        myResult = null;
    }
}
