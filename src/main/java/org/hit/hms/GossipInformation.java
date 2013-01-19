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

import java.util.concurrent.TimeUnit;

import org.hit.communicator.NodeID;

/**
 * Defines the contract for a <code>HealthRecord</code> that can be used for
 * bookkeeping about the health of a node.
 * 
 * @author Balraja Subbiah
 */
public class GossipInformation
{
    /**
     * This is the frequency by which nodes exchange gossip about other
     * nodes with a randomnly chosen neighbour.
     */
    public static final int GOSSIP_INTERVAL_IN_SECONDS = 1;
    
    /**
     * The number of gossip cycles after which a node is considered dead.
     */
    public static final int CLEANUP_TIME = 10;
   
    /**
     * The target node that's monitored.
     */
    private final NodeID myMonitoredNodeID;
    
    /**
     * The number of gossip cycles that has elapsed since we last received
     * from this node.
     */
    private long myGossipCyclesElapsed;
    
    /**
     * The time at which last gossip message was received from this node.
     */
    private long myLastGossipUpdateTime;

    /**
     * CTOR
     */
    public GossipInformation(NodeID monitoredNodeID)
    {
        myMonitoredNodeID = monitoredNodeID;
        update(System.currentTimeMillis());
    }

    /**
     * Returns the value of gossipCyclesElapsed
     */
    public long getGossipCyclesElapsed()
    {
        return myGossipCyclesElapsed;
    }

    /**
     * Returns the value of lastGossipUpdateTime
     */
    public long getLastGossipUpdateTime()
    {
        return myLastGossipUpdateTime;
    }

    /**
     * Returns the value of monitoredNodeID
     */
    public NodeID getMonitoredNodeID()
    {
        return myMonitoredNodeID;
    }
    
    /**
     * Updates gossip information about a particular node.
     */
    public void update(long lastUpdatedTime)
    {
        if (   myLastGossipUpdateTime != 0L
            && lastUpdatedTime < myLastGossipUpdateTime)
        {
            return;
        }
        myGossipCyclesElapsed =
            (System.currentTimeMillis() - lastUpdatedTime)
                / (TimeUnit.SECONDS.toMillis(GOSSIP_INTERVAL_IN_SECONDS));
        myLastGossipUpdateTime = lastUpdatedTime;
    }
    
    /**
     * Updates the no of gossip cycles elapsed since we last heard from
     * this node.
     */
    public void updateElapsedGossipCycles()
    {
        myGossipCyclesElapsed =
            (System.currentTimeMillis() - myLastGossipUpdateTime)
                / TimeUnit.SECONDS.toMillis(GOSSIP_INTERVAL_IN_SECONDS);
    }
}
