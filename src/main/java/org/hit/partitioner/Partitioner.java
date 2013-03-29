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

import java.io.Externalizable;
import java.util.Collection;

import org.hit.communicator.NodeID;

/**
 * Defines the contract for partitoning the <code>KeySpace</code> among
 * the <code>NodeId</code>
 * 
 * @author Balraja Subbiah
 */
public interface Partitioner<T extends Comparable<? super T>>
   extends Externalizable
{
    /**
     * A helper method to distribute the nodes among the key space.
     */
    public void distribute(Collection<NodeID> nodes);
    
    /**
     * Returns the <code>NodeID</code> corresponding to the key.
     */
    public NodeID getNode(T keyValue);
}
