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

package org.hit.messages;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Schema;
import org.hit.node.PartitionTable;

/**
 * Defines the allocation of points on key space corresponding to the
 * new node.
 * 
 * @author Balraja Subbiah
 */
public class Allocation implements Externalizable
{
    private Map<String, Schema> myTable2SchemaMap;
    
    private Map<String, PartitionTable<?,?>> myTable2PartitionMap;
    
    /**
     * CTOR
     */
    public Allocation()
    {
        myTable2PartitionMap = new HashMap<>();
        myTable2SchemaMap = new HashMap<>();
    }

    /**
     * CTOR
     */
    public Allocation(Map<String, Schema> table2SchemaMap,
                      Map<String, PartitionTable<?,?>> table2PartitionMap)
    {
        super();
        myTable2SchemaMap = table2SchemaMap;
        myTable2PartitionMap = table2PartitionMap;
    }

    /**
     * Returns the value of table2SchemaMap
     */
    public Map<String, Schema> getTable2SchemaMap()
    {
        return myTable2SchemaMap;
    }

    /**
     * Returns the value of table2PartitionMap
     */
    public Map<String, PartitionTable<?,?>> getTable2PartitionMap()
    {
        return myTable2PartitionMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myTable2PartitionMap);
        out.writeObject(myTable2SchemaMap);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,ClassNotFoundException
    {
        myTable2PartitionMap = 
           (Map<String, PartitionTable<?,?>>) in.readObject();
        myTable2SchemaMap = (Map<String, Schema>) in.readObject();
    }
}