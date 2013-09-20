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
import org.hit.time.Clock;

/**
 * Defines an abstract implementation of a <code>Transaction</code>
 * that takes care of capturing the references to the objects read
 * or written.
 *
 * @author Balraja Subbiah
 */
public abstract class AbstractTransaction implements Transaction
{
    private final DatabaseAdaptor myAdaptedDatabase;

    private final Clock myClock;

    private long myEndTime;

    private long myStartTime;

    private TransactionState myState;

    private final TransactionID myTransactionID;

    /**
     * CTOR
     */
    public AbstractTransaction(TransactionID transactionId,
                               TransactableDatabase database,
                               Clock clock)
    {
        myState = TransactionState.NOT_STARTED;
        myTransactionID = transactionId;
        myAdaptedDatabase =
            new DatabaseAdaptor(database, myTransactionID.getIdentifier());
        myClock = clock;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void abort()
    {
        myState = TransactionState.ABORTED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit()
    {
        doCommit(myAdaptedDatabase);
        myState = TransactionState.COMMITTED;
    }

    /**
     * Subclasses should override this implementation to execute their
     * implementation of commit.
     */
    protected abstract void doCommit(DatabaseAdaptor adpator);

    /**
     * Subclasses should override this method to execute their implementation
     * of <code>Transaction</code>
     */
    protected abstract void doExecute(Database database);

    /**
     * Subclasses should override this method to execute their implementation
     * of validating a transaction.
     */
    protected abstract boolean doValidation(DatabaseAdaptor adaptor);

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute()
    {
        myState = TransactionState.ACTIVE;
        Registry.updateTransactionState(myTransactionID.getIdentifier(), 
                                        myState);
        doExecute(myAdaptedDatabase);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getEndTime()
    {
        return myEndTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TransactionState getMyState()
    {
        return myState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getStartTime()
    {
        return myStartTime;
    }

    /**
     * Returns the value of transactionID
     */
    public TransactionID getTransactionID()
    {
        return myTransactionID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init()
    {
        myStartTime = myClock.currentTime();
        myAdaptedDatabase.setTransactionTime(myStartTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate()
    {
        myEndTime = myClock.currentTime();
        myState = TransactionState.VALIDATE;
        Registry.updateTransactionState(myTransactionID.getIdentifier(), 
                                        myState);
        return doValidation(myAdaptedDatabase);
    }
}
