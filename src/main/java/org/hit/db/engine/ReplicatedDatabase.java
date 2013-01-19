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

package org.hit.db.engine;

import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Database;
import org.hit.db.model.PartitioningType;
import org.hit.db.model.Persistable;
import org.hit.db.model.Schema;
import org.hit.db.model.Table;

/**
 * Defines the contract for the replicated database, wherein replicated
 * tables are stored.
 * 
 * @author Balraja Subbiah
 */
public class ReplicatedDatabase implements Database
{
    private final Map<String, Table<?,?>> myTables;
    
    private final Map<String, Schema> myNameToSchemaMap;
    
    /**
     * CTOR
     */
    public ReplicatedDatabase()
    {
        myTables = new HashMap<>();
        myNameToSchemaMap = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(Schema schema)
    {
        myNameToSchemaMap.put(schema.getTableName(), schema);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <K extends Comparable<K>, P extends Persistable<K>> Table<K, P>
        lookUpTable(String tableName)
    {
        Table<K,P> table = (Table<K, P>) myTables.get(tableName);
        if (table == null) {
            Schema schema = myNameToSchemaMap.get(tableName);
            if (schema != null) {
                table =
                    (schema.getKeyPartitioningType() == PartitioningType.HASHABLE) ?
                        new ReplicatedHashedTable<K,P>(schema)
                        : new ReplicatedPartitionedTable<K,P>(schema);
                myTables.put(tableName, table);
            }
        }
        return table;
    }
}
