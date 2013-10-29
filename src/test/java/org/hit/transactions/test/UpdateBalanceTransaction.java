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

package org.hit.transactions.test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.db.model.Database;
import org.hit.db.model.Mutation;
import org.hit.db.model.Table;

/**
 * A simple mutation to update the account balance.
 * 
 * @author Balraja Subbiah
 */
public class UpdateBalanceTransaction implements Mutation
{
    private final long myAccountID;
    
    private final double myBalance;

    /**
     * CTOR
     */
    public UpdateBalanceTransaction(long accountID, double balance)
    {
        super();
        myAccountID = accountID;
        myBalance = balance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Database database)
    {
        Table<Long,Account> accountTable = 
            database.lookUpTable(Account.TABLE_NAME);
        Account account = accountTable.getRow(Long.valueOf(myAccountID));
        if (account == null) {
            accountTable.update(null, new Account(myAccountID, myBalance));
        }
        else {
            accountTable.update(account, 
                                new Account(myAccountID, 
                                            account.getBalance() + myBalance));
        }
    }
}
