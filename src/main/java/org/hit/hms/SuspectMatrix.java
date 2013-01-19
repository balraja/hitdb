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

package org.hit.hms;

import java.util.HashMap;
import java.util.Map;

import org.hit.communicator.NodeID;

/**
 * The matrix of all <code>SuspectVector</code>s of all nodes as seen by this
 * node.
 * 
 * @author Balraja Subbiah
 */
public class SuspectMatrix
{
    private final NodeID myNodeID;
    
    private final Map<NodeID, SuspectVector> mySuspectVectors;
    
    /**
     * CTOR
     */
    public SuspectMatrix(NodeID nodeID, SuspectVector suspectVector)
    {
        mySuspectVectors = new HashMap<>();
        myNodeID = nodeID;
        mySuspectVectors.put(nodeID, suspectVector);
    }
    
    /**
     * Adds a <code>SuspectVector</code> from the given <code>NodeID</code>
     * received through gossip protocol.
     */
    public void addSuspectVector(NodeID nodeID, SuspectVector suspectVector)
    {
        mySuspectVectors.put(nodeID, suspectVector);
        if (!nodeID.equals(myNodeID)) {
           SuspectVector localSuspectVector =  mySuspectVectors.get(myNodeID);
           localSuspectVector.merge(suspectVector);
        }
    }
    
    /**
     * Returns the <code>SuspectVector</code> of a given <code>NodeID</code>
     * as seen by this node.
     */
    public SuspectVector getSuspectVector(NodeID nodeID)
    {
        return mySuspectVectors.get(nodeID);
    }
    
    /**
     * A helper method to update intervals that has elapsed since we last heard
     * from  that node.
     */
    public void updateElapsedGossipCycles()
    {
        for (SuspectVector vector : mySuspectVectors.values()) {
            vector.updateElapsedGossipCycles();
        }
    }
}
