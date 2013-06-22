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

package org.hit.node;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.TreeMap;

import org.hit.communicator.NodeID;
import org.hit.gossip.AbstractGossip;
import org.hit.key.Keyspace;
import org.hit.util.Pair;

/**
 * Defines the contract for the table that defines the partitions.
 * 
 * @author Balraja Subbiah
 */
public abstract class PartitionTable<S extends Comparable<S>, 
                                     T extends Comparable<T>>
    extends AbstractGossip<Pair<T, NodeID>>
{
    private TreeMap<T, NodeID> myKeyToNodeMap;
    
    private Keyspace<S, T> myKeyspace;
    
    /**
     * CTOR
     */
    public PartitionTable()
    {
        super();
        myKeyToNodeMap = null;
        myKeyspace = null;
    }
    
    /**
     * CTOR
     */
    public PartitionTable(String tableName, Keyspace<S, T> keyspace)
    {
        super(tableName);
        myKeyToNodeMap = new TreeMap<>();
        myKeyspace = keyspace;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void doUpdate(Pair<T, NodeID> update)
    {
        myKeyToNodeMap.put(update.getFirst(), update.getSecond());
    }
    
    /**
     * Returns the <code>NodeID</code> which holds the partition containing 
     * the given key's value.
     */
    public NodeID lookupNode(S key)
    {
        if (myKeyToNodeMap == null || myKeyToNodeMap.isEmpty()) {
            return null;
        }
        return doLookup(myKeyspace.map(key), myKeyToNodeMap);
    }
    
    /**
     * Subclasses should override this method for performing node lookup.
     */
    protected abstract NodeID doLookup(T                  key, 
                                       TreeMap<T, NodeID> nodeMap);

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myKeyToNodeMap);
        out.writeObject(myKeyspace);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myKeyToNodeMap = (TreeMap<T, NodeID>) in.readObject();
        myKeyspace = (Keyspace<S, T>) in.readObject();
    }
}
