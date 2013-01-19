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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.hit.communicator.NodeID;
import org.hit.topology.Topology;

/**
 * Defines the contract for book keeping information kept at a particular
 * node wrt it's suspicions regarding the other nodes.
 * 
 * @author Balraja Subbiah
 */
public class SuspectVector implements Externalizable
{
    private Map<NodeID, GossipInformation> myGossipInformation;
    
    /**
     * CTOR
     */
    public SuspectVector()
    {
        myGossipInformation = new HashMap<>();
    }
    
    /**
     * CTOR
     */
    public SuspectVector(Topology topology)
    {
        myGossipInformation = new HashMap<>();
        for (NodeID nodeID : topology.getParticipatingNodes()) {
            myGossipInformation.put(nodeID,
                                    new GossipInformation(nodeID));
        }
    }
    
    /** Returns all gossip stored in this suspect vector */
    public Collection<GossipInformation> getAllGossip()
    {
        return myGossipInformation.values();
    }
    
    /**
     * Returns the <code>GossipInformation</code> about a node.
     */
    public GossipInformation getSuspectInformation(NodeID nodeID)
    {
        return myGossipInformation.get(nodeID);
    }
    
    /**
     * Merge the gossip information from the incoming suspect vector
     * with this suspect vector.
     */
    public void merge(SuspectVector vector)
    {
        for (GossipInformation gossip : vector.getAllGossip()) {
            GossipInformation localGossip =
                myGossipInformation.get(gossip.getMonitoredNodeID());
            if (localGossip != null) {
                localGossip.update(gossip.getLastGossipUpdateTime());
            }
            else {
                myGossipInformation.put(gossip.getMonitoredNodeID(),
                                        gossip);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myGossipInformation = (Map<NodeID, GossipInformation>) in.readObject();
    }
    
    /**
     * A helper method to update intervals that has elapsed since we last heard
     * from  that node.
     */
    public void updateElapsedGossipCycles()
    {
        for (GossipInformation gi : myGossipInformation.values()) {
            gi.updateElapsedGossipCycles();
        }
    }

    /**
     * A helper method to update the last gossiped time.
     */
    public void updateLastGossipedTime(NodeID node)
    {
        GossipInformation gossip = myGossipInformation.get(node);
        gossip.update(System.currentTimeMillis());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myGossipInformation);
    }
}
