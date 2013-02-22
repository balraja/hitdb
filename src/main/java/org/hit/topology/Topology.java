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

package org.hit.topology;

import java.util.List;

import org.hit.communicator.NodeID;

/**
 * Defines the contract that defines the organization in which nodes in the
 * database are organized. The topology can initially be empty for a new node.
 * We can then ping the seed node to get the updated topology and also join
 * the distributed service.
 * 
 * @author Balraja Subbiah
 */
public interface Topology
{
    /**
     * Returns the list of nodes that are currently part of hit cluster.
     */
    public List<NodeID> getNodes();
    
    /**
     * Returns the list of other neighbors that are participating in a
     * distributed database other than this node.
     */
    public List<NodeID> getParticipatingNodes();
    
    /**
     * Returns the node on which the given node's data is replicated.
     */
    public NodeID getReplicatingNodeID(NodeID nodeID);
    
    /**
     * Returns the node whose data is replicated on the given node.
     */
    public NodeID getReplicaSource(NodeID nodeID);
}
