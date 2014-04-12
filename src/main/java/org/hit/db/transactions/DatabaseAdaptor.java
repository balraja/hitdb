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

import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Database;
import org.hit.db.model.HitTableSchema;
import org.hit.db.model.Persistable;
import org.hit.db.model.Table;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * An adaptor to match {@link TransactableDatabase} to the requirements of
 * {@link Database}
 * 
 * @author Balraja Subbiah
 */
public class DatabaseAdaptor implements Database,Poolable
{
    private TransactableDatabase                 myDatabase;
    
    private final Map<String, TableAdaptor<?,?>> myCachedTables;
    
    private long                                 myTransactionTime;
    
    private long                                 myTransactionId;
    
    /**
     * CTOR
     */
    public DatabaseAdaptor()
    {
        myCachedTables = new HashMap<String, TableAdaptor<?,?>>();
    }

    /**
     * CTOR
     */
    public static DatabaseAdaptor create(
        TransactableDatabase database, long transId)
    {
        DatabaseAdaptor adaptor = PooledObjects.getInstance(DatabaseAdaptor.class);
        adaptor.myDatabase = database;
        adaptor.myTransactionTime = Transaction.CONST_NULL_TIME;
        adaptor.myTransactionId = transId;
        return adaptor;
    }
    
    /**
     * Commits the updates made on the database by the transaction.
     */
    public void commit(long commitTime)
    {
        for (TableAdaptor<?, ?> adaptor : myCachedTables.values()) {
            adaptor.commit(commitTime);
        }
    }
    
    /**
     * Removes the updates made on the database by the transaction.
     */
    public void abort()
    {
        for (TableAdaptor<?, ?> adaptor : myCachedTables.values()) {
            adaptor.abort();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(HitTableSchema schema)
    {
        myDatabase.createTable(schema);
    }

    /**
     * Returns the value of cachedTables
     */
    public Map<String, TableAdaptor<?, ?>> getCachedTables()
    {
        return myCachedTables;
    }

    /**
     * Returns the value of database
     */
    public TransactableDatabase getDatabase()
    {
        return myDatabase;
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <K extends Comparable<K>, P extends Persistable<K>> Table<K, P>
        lookUpTable(String tableName)
    {
        TableAdaptor<K, P> cachedAdaptor =
           (TableAdaptor<K, P>) myCachedTables.get(tableName);
        
        if (cachedAdaptor == null) {
            TransactableTable<K, P> tt = myDatabase.lookUpTable(tableName);
            cachedAdaptor =
                TableAdaptor.<K, P>create(tt, myTransactionTime, myTransactionId);
            myCachedTables.put(tableName, cachedAdaptor);
        }
        return cachedAdaptor;
    }
    
    /**
     * Returns {@link TransactionTableTrail} that cpatures the reads and
     * writes performed by {@link Transaction} on a database.
     */
    public <K extends Comparable<K>, P extends Persistable<K>>
        TransactionTableTrail<K, P> lookupTableTrail(String tableName)
    {
        @SuppressWarnings("unchecked")
        TableAdaptor<K,P> adaptor =
            (TableAdaptor<K, P>) myCachedTables.get(tableName);
        
        return (adaptor != null) ? adaptor.getTableTrail() : null;
    }

    /**
     * Setter for the transactionTime
     */
    public void setTransactionTime(long transactionTime)
    {
        myTransactionTime = transactionTime;
    }
    
    /**
     * Returns true if the <code>TransactionValidator</code> is valid against
     * this database.
     */
    public boolean validate(TransactionValidator validator)
    {
        for (TableAdaptor<?, ?> adaptor : myCachedTables.values()) {
            if (!adaptor.validate(validator)) {
                return false;
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myDatabase = null;
        myTransactionId = Long.MIN_VALUE;
        myTransactionId = Long.MIN_VALUE;
    }
}
