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

package org.hit.db.transactions.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.hit.db.keyspace.HashKeyspace;
import org.hit.db.model.Persistable;
import org.hit.db.model.HitTableSchema;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.TransactableTable;
import org.hit.event.DBStatEvent;
import org.hit.util.LogFactory;

/**
 * Defines an implementation for the hit database that stores the
 * <code>Table</code>s
 *
 * @author Balraja Subbiah
 */
public class TransactableHitDatabase implements TransactableDatabase
{
    /** LOGGER */
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(TransactableHitDatabase.class);
    
    private final Map<String, TransactableTable<?, ?>> myDatabaseTables;

    private final Map<String, HitTableSchema> myTable2Schema;
    
    private final AtomicLong myLock;

    /**
     * CTOR
     */
    public TransactableHitDatabase()
    {
        myDatabaseTables = new HashMap<>();
        myTable2Schema = new HashMap<>();
        myLock = new AtomicLong(UNLOCKED_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(HitTableSchema schema)
    {
        // Creates table lazily. It just stores schema against the table
        // name and creates the table only when it's required by the
        // transactions.
        myTable2Schema.put(schema.getTableName(), schema);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <K extends Comparable<K>, P extends Persistable<K>>
        TransactableTable<K, P> lookUpTable(String tableName)
    {
        TransactableTable<K,P> table =
            (TransactableTable<K,P>) myDatabaseTables.get(tableName);

        if (table == null) {
            table = makeTable(myTable2Schema.get(tableName));
            myDatabaseTables.put(tableName, table);
        }
        return table;
    }

    private <K extends Comparable<K>, P extends Persistable<K>>
        TransactableTable<K, P> makeTable(HitTableSchema schema)
    {
        if (schema.getKeyspace() instanceof HashKeyspace)
        {
            return new TransactableHashedTable<>(schema);
        }
        else {
            return new TransactablePartitionedTable<>(schema);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DBStatEvent getStatistics()
    {
        DBStatEvent stat = new DBStatEvent();
        for (String tableName : myTable2Schema.keySet()) {
            TransactableTable<?, ?> table = myDatabaseTables.get(tableName);
            if (table != null) {
                stat.addTableRowCount(tableName, table.rowCount());
            }
            else {
                stat.addTableRowCount(tableName, 0L);
            }
        }
        return stat;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean lock(long transactionID)
    {
        LOG.info("The transaction " + transactionID + " has requested for "
                 + " locking the database");
        return myLock.compareAndSet(UNLOCKED_VALUE, transactionID);   
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean unlock(long transactionID)
    {
        return myLock.compareAndSet(transactionID, UNLOCKED_VALUE);      
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean canProcess(long transactionID)
    {
        return (   myLock.get() == UNLOCKED_VALUE
                || myLock.get() == transactionID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getLockedTransaction()
    {
        return myLock.get();
    }
}

