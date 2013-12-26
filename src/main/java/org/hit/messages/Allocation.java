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

import org.hit.communicator.NodeID;
import org.hit.db.model.Schema;
import org.hit.db.partitioner.Partitioner;

/**
 * Defines the allocation of points on key space corresponding to the
 * new node.
 * 
 * @author Balraja Subbiah
 */
public class Allocation implements Externalizable
{
    private Map<String, Schema> myTable2SchemaMap;
    
    private Map<String, Partitioner<?,?>> myTable2PartitionMap;
    
    private Map<String, NodeID> myTableToDataNodeMap;
    
    /**
     * CTOR
     */
    public Allocation()
    {
        myTable2PartitionMap = new HashMap<>();
        myTable2SchemaMap = new HashMap<>();
        myTableToDataNodeMap = new HashMap<>();
    }

    /**
     * CTOR
     */
    public Allocation(Map<String, Schema> table2SchemaMap,
                      Map<String, Partitioner<?,?>> table2PartitionMap,
                      Map<String, NodeID> tableToDataNodeMap)
    {
        super();
        myTable2SchemaMap = table2SchemaMap;
        myTable2PartitionMap = table2PartitionMap;
        myTableToDataNodeMap = tableToDataNodeMap;
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
    public Map<String, Partitioner<?,?>> getTable2PartitionMap()
    {
        return myTable2PartitionMap;
    }
    
    /**
     * Returns the value of tableToDataNodeMap
     */
    public Map<String, NodeID> getTableToDataNodeMap()
    {
        return myTableToDataNodeMap;
    }
    
    /**
     * Returns true if master has allocated data, that in turn has to be 
     * fetched from other nodes.
     */
    public boolean shouldFetchDataFromOtherNodes()
    {
        return !myTableToDataNodeMap.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myTable2PartitionMap);
        out.writeObject(myTable2SchemaMap);
        out.writeObject(myTableToDataNodeMap);
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
           (Map<String, Partitioner<?,?>>) in.readObject();
        myTable2SchemaMap = (Map<String, Schema>) in.readObject();
        myTableToDataNodeMap = (Map<String, NodeID>) in.readObject();
    }
}
