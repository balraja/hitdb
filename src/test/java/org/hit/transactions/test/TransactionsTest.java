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

import org.hit.db.model.Mutation;
import org.hit.db.model.Query;
import org.hit.db.transactions.AbstractTransaction;
import org.hit.db.transactions.ReadTransaction;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.WriteTransaction;
import org.hit.db.transactions.impl.TransactableHitDatabase;
import org.hit.time.Clock;
import org.hit.time.SimpleSystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * A simple testcase to verify the transaction execution.
 * 
 * @author Balraja Subbiah
 */
public class TransactionsTest
{
    private TransactableDatabase myDatabase;
    
    private Clock                myClock;
    
    private long                 myTransactionID;
    
    @Before
    public void setupTest()
    {
        myDatabase      = new TransactableHitDatabase();
        myClock         = new SimpleSystemClock();
        myTransactionID = 1L;
        
        myDatabase.createTable(Account.SCHEMA);
    }
    
    private void apply(Mutation mutation, boolean isCommit)
    {
        AbstractTransaction transaction = 
            WriteTransaction.create(myTransactionID++,
                                    myDatabase,
                                    myClock, 
                                    mutation);
        
        transaction.init();
        transaction.execute();
        if (isCommit) {
            transaction.commit();
        }
        else {
            transaction.abort();
        }
    }
    
    private Object execute(Query query)
    {
        ReadTransaction transaction = 
            ReadTransaction.create(myTransactionID++,
                                   myDatabase,
                                   myClock, 
                                   query);
        
        transaction.init();
        transaction.execute();
        return transaction.getResult();
    }
    
    @Test
    public void updateAndQueryTest()
    {
        apply(new UpdateBalanceTransaction(1L, 100.0D), true);
        Object result = execute(new BalanceQuery(1L));
        Assert.assertNotNull(result);
        Assert.assertEquals(100.0D, (Double) result, 0.0D);
    }
    
    @Test
    public void abortQueryTest()
    {
        apply(new UpdateBalanceTransaction(1L, 100.0D), true);
        apply(new UpdateBalanceTransaction(1L, 200.0D), false);
        Object result = execute(new BalanceQuery(1L));
        Assert.assertNotNull(result);
        Assert.assertEquals(100.0D, (Double) result, 0.0D);
    }
}
