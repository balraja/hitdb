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

package org.hit.distribution;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.hit.communicator.NodeID;

/**
 * Defines the contract for the key space that's partitioned between the
 * nodes in a linear fashion.
 * 
 * @author Balraja Subbiah
 */
public class LinearPartitioner<T extends Comparable<? super T>>
    implements KeyPartitioner<T>
{
    private final KeySpace<T> myKeySpace;
    
    private final TreeMap<T, NodeID> myKeyToNodeMap;
    
    private final Map<NodeID, T> myNodeToMinValue;
    
    /**
     * CTOR
     */
    public LinearPartitioner(KeySpace<T> keyspace)
    {
        myKeySpace = keyspace;
        myKeyToNodeMap = new TreeMap<>();
        myNodeToMinValue = new HashMap<NodeID, T>();
    }
    
    /** {@inheritDoc} */
    @Override
    public void distribute(Collection<NodeID> nodes)
    {
        long offset = myKeySpace.getTotalElements() / nodes.size();
        T minValue = myKeySpace.getMinimum();
        T nodeValue = myKeySpace.nextAtOffset(minValue, offset);
        for (NodeID node : nodes) {
            myKeyToNodeMap.put(nodeValue, node);
            myNodeToMinValue.put(node, minValue);
            minValue = nodeValue;
            nodeValue = myKeySpace.nextAtOffset(minValue, offset);
            if (nodeValue.compareTo(
                    myKeySpace.nextAtOffset(nodeValue, offset)) > 0)
            {
                nodeValue = myKeySpace.getMaximum();
            }
        }
    }
    
    /**
     * Returns the min value corresponding to the given range gaurded by the
     * <code>NodeID</code>
     */
    public T getMinValue(NodeID nodeId)
    {
        return myNodeToMinValue.get(nodeId);
    }
    
    /** {@inheritDoc} */
    @Override
    public NodeID getNode(T value)
    {
        return myKeyToNodeMap.ceilingEntry(value).getValue();
    }
}
