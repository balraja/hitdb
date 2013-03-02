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
import org.hit.time.Clock;

/**
 * Defines the contract for the write transactions to be performed on a
 * database.
 *
 * @author Balraja Subbiah
 */
public class WriteTransaction extends AbstractTransaction
{
    private final Mutation myMutation;

    /**
     * CTOR
     */
    public WriteTransaction(TransactionID transactionId,
                            TransactableDatabase database,
                            Clock clock,
                            Mutation mutation)
    {
        super(transactionId, database, clock);
        myMutation = mutation;

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
            new WriteTransactionValidator(adaptor.getDatabase(),
                                          getEndTime(),
                                          getTransactionID().getIdentifier());
        return adaptor.validate(wtv);
    }

    /**
     * Returns the value of mutation
     */
    public Mutation getMutation()
    {
        return myMutation;
    }

}
