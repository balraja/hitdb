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

package org.hit.topology;

import java.util.Collections;
import java.util.List;

import org.hit.communicator.NodeID;

import com.google.inject.Inject;

/**
 * Implements <code>Topology</code> that wraps a single node.
 * 
 * @author Balraja Subbiah
 */
public class SingletonTopology implements Topology
{
    private final NodeID myNodeID;
    
    /**
     * CTOR
     */
    @Inject
    public SingletonTopology(NodeID nodeID)
    {
        myNodeID = nodeID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeID> getNodes()
    {
        return Collections.singletonList(myNodeID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeID> getParticipatingNodes()
    {
        return Collections.emptyList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeID getReplicatingNodeID(NodeID nodeID)
    {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeID getReplicaSource(NodeID nodeID)
    {
        return null;
    }
}
