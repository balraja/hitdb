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
import org.hit.time.Clock;

/**
 * Defines the contract for a <code>Transaction</code> that performs only
 * the querying operation on a database.
 * 
 * @author Balraja Subbiah
 */
public class ReadTransaction extends AbstractTransaction
{
    private final Query myQuery;
    
    private Object myResult;

    /**
     * CTOR
     */
    public ReadTransaction(TransactionID transactionId,
                           TransactableDatabase database,
                           Clock clock,
                           Query query)
    {
        super(transactionId, database, clock);
        myQuery = query;
        myResult = null;
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
            new ReadTransactionValidator(adaptor.getDatabase(),
                                         getEndTime(),
                                         getTransactionID().getIdentifier());
        return adaptor.validate(validator);
    }

    /**
     * Returns the value of result
     */
    public Object getResult()
    {
        return myResult;
    }
}
