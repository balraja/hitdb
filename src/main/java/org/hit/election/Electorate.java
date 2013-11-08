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
package org.hit.election;

import com.google.common.eventbus.EventBus;

import java.util.List;

import org.hit.communicator.NodeID;
import org.hit.zookeeper.ZooKeeperClient;

/**
 * Defines the set nodes that participates in a election.
 * 
 * @author Balraja Subbiah
 */
public class Electorate
{
    /**
     * A listener to be used for notifying about events
     * of interest wrt the electorate.
     */
    public static interface Listener
    {
        /**
         * Notifies that leader is down.
         */
        public void notifyLeaderDown(ElectorateID electorateID);
    }
    
    private final ElectorateID myID;
    
    private final long myTerm;
    
    private final ZooKeeperClient myZKClient;
    
    private final String myZKPath;

    /**
     * CTOR
     */
    public Electorate(ElectorateID id, ZooKeeperClient zc, EventBus eventBus)
    {
        myID    = id;
        myTerm  = 0L;
        myZKClient = zc;
        myZKPath = ZooKeeperClient.PATH_SEPARATOR + id.toString();
    }
    
    /**
     * Adds a node to the electorate.
     */
    public void add(NodeID node, boolean isLeader)
    {
        myZKClient.addNode(myID.toString(), node);
        if (isLeader) {
            myZKClient.acquireLockUnder(myZKPath, 
                                        node);
        }
    }

    /**
     * Returns the {@link ElectorateID} for this electorate.
     */
    public ElectorateID getID()
    {
        return myID;
    }

    /**
     * Returns the value of nodes
     */
    public List<NodeID> getNodes()
    {
        return myZKClient.getNodes(myZKPath);
    }

    /**
     * Returns the value of term
     */
    public long getTerm()
    {
        return myTerm;
    }
}
