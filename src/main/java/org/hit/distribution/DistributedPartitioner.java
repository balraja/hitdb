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

package org.hit.distribution;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.hit.communicator.NodeID;

/**
 * Defines the contract for a distributed hash table, wherein each node claims
 * the entries in a ring and services the data between previous node's entry
 * and current node's entry.
 * 
 * @author Balraja Subbiah
 */
public class DistributedPartitioner<T extends Comparable<? super T>>
    implements KeyPartitioner<T>
{
    private final KeySpace<T> myHashRing;
    
    private final TreeMap<T, NodeID> myClaimedPositions;
    
    /**
     * CTOR
     */
    public DistributedPartitioner(KeySpace<T> hashRing)
    {
        myHashRing = hashRing;
        myClaimedPositions = new TreeMap<>();
    }
    
    /** Distributes the nodes over a hash ring */
    @Override
    public void distribute(Collection<NodeID> nodes)
    {
        long offset = myHashRing.getTotalElements() / nodes.size();
        T nodeValue = myHashRing.nextAtOffset(myHashRing.getMinimum(), offset);
        for (NodeID node : nodes) {
            myClaimedPositions.put(nodeValue, node);
            nodeValue = myHashRing.nextAtOffset(nodeValue, offset);
        }
    }

    /**
     * Returns the value of claimedPositions
     */
    public Map<T, NodeID> getClaimedPositions()
    {
        return Collections.<T, NodeID>unmodifiableMap(myClaimedPositions);
    }

    /**
     * Returns the value of hashRing
     */
    public KeySpace<T> getHashRing()
    {
        return myHashRing;
    }
    
    /** Returns the node corresponding to the given key */
    @Override
    public NodeID getNode(T key)
    {
        NodeID holdingNode = myClaimedPositions.get(key);
        if (holdingNode == null) {
            Map.Entry<T,NodeID> nextEntry =
                myClaimedPositions.ceilingEntry(key);
            if (nextEntry != null) {
                holdingNode = nextEntry.getValue();
            }
            else {
               T maxKey =  myClaimedPositions.lastKey();
               if (maxKey.compareTo(key) < 0) {
                   nextEntry =
                       myClaimedPositions.ceilingEntry(myHashRing.getMinimum());
                   
                   if (nextEntry != null) {
                       holdingNode = nextEntry.getValue();
                   }
               }
            }
        }
        return holdingNode;
    }
}
