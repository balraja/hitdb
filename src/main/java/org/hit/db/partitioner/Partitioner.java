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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.hit.communicator.NodeID;
import org.hit.db.keyspace.Keyspace;
import org.hit.gossip.AbstractGossip;
import org.hit.util.Pair;
import org.hit.util.Range;

/**
 * Defines the contract for a tye that defines the partitions of 
 * keyspace among all the nodes.
 * 
 * @author Balraja Subbiah
 */
public class Partitioner<S extends Comparable<S>, 
                         T extends Comparable<T>>
    extends AbstractGossip<Pair<Comparable<?>, NodeID>>
{
    private TreeMap<T, NodeID> myKeyToNodeMap;
    
    private Keyspace<S, T> myKeyspace;
    
    /**
     * CTOR
     */
    public Partitioner()
    {
        super();
        myKeyToNodeMap = null;
        myKeyspace = null;
    }
    
    /**
     * CTOR
     */
    public Partitioner(String tableName, Keyspace<S, T> keyspace)
    {
        super(tableName);
        myKeyToNodeMap = new TreeMap<>();
        myKeyspace = keyspace;
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    protected void doUpdate(Pair<Comparable<?>, NodeID> update)
    {
        myKeyToNodeMap.put(((T)update.getFirst()), update.getSecond());
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
     * Returns the range of keys corresponding to this node.
     */
    public Range<T> getNodeRange(NodeID nodeID)
    {
        Map.Entry<T, NodeID> oldEntry = null;
        for (Map.Entry<T, NodeID> entry : myKeyToNodeMap.entrySet())
        {
            if (entry.getValue().equals(nodeID)) {
                if (oldEntry != null) {
                    return new Range<>(oldEntry.getKey(), entry.getKey());
                }
                else {
                    return new Range<>(myKeyspace.getDomain().getMinimum(), 
                                       entry.getKey());
                }
            }
            oldEntry = entry;
        }
        return null;
    }
    
    /**
     * Returns the map between {@link NodeID} and the sliced key ranges 
     * that map to the nodes.
     */
    public Map<NodeID, Range<T>> lookupNodes(Range<S> sourceRange)
    {
        Range<T> range = new Range<T>(myKeyspace.map(sourceRange.getMinValue()),
                                      myKeyspace.map(sourceRange.getMaxValue()));
        
        Map<NodeID,Range<T>> result = new HashMap<NodeID, Range<T>>();
        T oldKey = null;
        for (Map.Entry<T, NodeID> entry : myKeyToNodeMap.entrySet()) {
            Range<T> nodeRange = 
                oldKey == null ? new Range<T>(myKeyspace.getDomain().getMinimum(),
                                              entry.getKey())
                               : new Range<T>(oldKey, entry.getKey());
                
            if (nodeRange.contains(range)) {
                result.put(entry.getValue(), range);
                break;
            }
            else if (nodeRange.leftOverlap(range)) {
                result.put(entry.getValue(), 
                           new Range<T>(range.getMinValue(),
                                        nodeRange.getMaxValue()));
            }
            else if (range.contains(nodeRange)) {
                result.put(entry.getValue(), nodeRange);
            }
            else if (nodeRange.rightOverlap(range)) {
                result.put(entry.getValue(), 
                           new Range<T>(nodeRange.getMinValue(),
                                        range.getMaxValue()));
            }
            oldKey = entry.getKey();
        }
        return result;
    }
    
    /**
     * Subclasses should override this method for performing node lookup.
     */
    protected NodeID doLookup(T                  key, 
                              TreeMap<T, NodeID> nodeMap)
    {
        Entry<T, NodeID> maxEntry = nodeMap.ceilingEntry(key);
        return maxEntry != null ? maxEntry.getValue() : null;
    }

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
