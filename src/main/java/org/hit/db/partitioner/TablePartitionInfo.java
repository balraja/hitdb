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
package org.hit.db.partitioner;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A simple class to keep track of the partition information 
 * among the nodes;
 * 
 * @author Balraja Subbiah
 */
public class TablePartitionInfo implements Externalizable
{
    private Map<String, Partitioner<?,?>> myTableToKeyPartitions;

    /**
     * CTOR
     */
    public TablePartitionInfo()
    {
        myTableToKeyPartitions = new HashMap<>();
    }

    /**
     * CTOR
     */
    public TablePartitionInfo(
        Map<String, Partitioner<?, ?>> tableToKeyPartitions)
    {
        super();
        myTableToKeyPartitions = tableToKeyPartitions;
    }
    
    /**
     * Merges the incoming partition information with the 
     * existing values.
     */
    public synchronized void merge(TablePartitionInfo other) 
    {
        myTableToKeyPartitions.putAll(other.myTableToKeyPartitions);
    }
    
    /**
     * Adds partitioning information about a table into the table partition
     * cache.
     */
    public synchronized void addPartitionInfo(String tableName, 
                                              Partitioner<?,?> partitionInfo)
    {
        myTableToKeyPartitions.put(tableName, partitionInfo);
    }
    
    /**
     * Returns the {@link Partitioner} for the given table.
     */
    @SuppressWarnings("unchecked")
    public synchronized <K extends Comparable<K>> Partitioner<K,?>
        lookup(String tableName)
    {
        return (Partitioner<K, ?>) myTableToKeyPartitions.get(tableName);
    }
    
    /**
     * Returns the {@link Partitioner} for the given table.
     */
    @SuppressWarnings("unchecked")
    public synchronized <K extends Comparable<K>> Partitioner<K,K>
        lookupLinearPartitioner(String tableName)
    {
        return (Partitioner<K, K>) myTableToKeyPartitions.get(tableName);
    }
    
    /**
     * Returns the list of table names.
     */
    public synchronized Set<String> getTableNames()
    {
        return Collections.unmodifiableSet(myTableToKeyPartitions.keySet());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myTableToKeyPartitions);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        myTableToKeyPartitions = 
            (Map<String, Partitioner<?, ?>>) in.readObject();
    }
}
