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

import java.util.ArrayList;

import org.hit.db.model.Column;
import org.hit.db.model.Persistable;
import org.hit.db.model.Row;
import org.hit.db.model.Schema;
import org.hit.key.LinearKeyspace;
import org.hit.key.domain.LongDomain;

/**
 * Defines type that defines the contract for an account.
 * 
 * @author Balraja Subbiah
 */
public class Account implements Persistable<Long>, Row
{
    public static final String TABLE_NAME = "account";
    
    public static final Schema SCHEMA = 
        new Schema(TABLE_NAME,
                   new ArrayList<Column>(),
                   Account.class, 
                   Long.class,
                   new LinearKeyspace<>(new LongDomain(1, 7000))); 
    
    private static final String ACCOUNT_ID = "account_id";
    
    private static final String BALANCE = "balance";
    
    private final long myAccountID;
    
    private final double myBalance;
    
    /**
     * CTOR
     */
    public Account(long accountID, double balance)
    {
        super();
        myAccountID = accountID;
        myBalance = balance;
    }
    
    /**
     * Returns the value of accountID
     */
    public long getAccountID()
    {
        return myAccountID;
    }

    /**
     * Returns the value of balance
     */
    public double getBalance()
    {
        return myBalance;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFieldValue(String fieldName)
    {
        switch(fieldName) {
        case ACCOUNT_ID:
            return Long.valueOf(getAccountID());
        case BALANCE:
            return Double.valueOf(getBalance());
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long primaryKey()
    {
        return Long.valueOf(getAccountID());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (myAccountID ^ (myAccountID >>> 32));
        long temp;
        temp = Double.doubleToLongBits(myBalance);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Account other = (Account) obj;
        if (myAccountID != other.myAccountID)
            return false;
        if (Double.doubleToLongBits(myBalance) != Double
                                                        .doubleToLongBits(other.myBalance))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Account [myAccountID=" + myAccountID
               + ", myBalance="
               + myBalance
               + "]";
    }
}
