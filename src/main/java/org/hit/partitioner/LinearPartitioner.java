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

package org.hit.partitioner;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.TreeMap;

import org.hit.communicator.NodeID;
import org.hit.partitioner.domain.DiscreteDomain;

/**
 * Defines the contract for the key space that's partitioned between the
 * nodes in a linear fashion.
 *
 * @author Balraja Subbiah
 */
public class LinearPartitioner<T extends Comparable<T>>
    implements Partitioner<T>
{
    private DiscreteDomain<T> myDomain;

    private TreeMap<T, NodeID> myKeyToNodeMap;

    /**
     * CTOR
     */
    public LinearPartitioner()
    {
        myDomain = null;
        myKeyToNodeMap = null;
    }

    /**
     * CTOR
     */
    public LinearPartitioner(DiscreteDomain<T> domain)
    {
        myDomain = domain;
        myKeyToNodeMap = new TreeMap<>();
    }

    /** {@inheritDoc} */
    @Override
    public void distribute(Collection<NodeID> nodes)
    {
        long segmentSize =
            myDomain.getTotalElements() / nodes.size();
        int index = 1;
        for (NodeID node : nodes) {
            long elementIndex = (index * segmentSize) - 1;
            T nodeValue = myDomain.elementAt(elementIndex);
            if (index == nodes.size()) {
                if (myDomain.getTotalElements() > elementIndex
                    && myDomain.getTotalElements() - elementIndex < segmentSize)
                {
                    nodeValue = myDomain.getMaximum();
                }
            }
            myKeyToNodeMap.put(nodeValue, node);
            index++;
        }
    }

    /**
     * Returns the possible enumerations of a key.
     */
    public DiscreteDomain<T> getDomain()
    {
        return myDomain;
    }


    /** {@inheritDoc} */
    @Override
    public NodeID getNode(T value)
    {
        return myKeyToNodeMap.ceilingEntry(value).getValue();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myDomain = (DiscreteDomain<T>) in.readObject();
        myKeyToNodeMap = new TreeMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myDomain);
    }
}
