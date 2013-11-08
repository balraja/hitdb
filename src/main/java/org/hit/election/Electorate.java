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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.hit.communicator.NodeID;
import org.hit.util.Pair;
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
        
        /**
         * Notifies that leader is available now.
         */
        public void notifyLeaderAvailability(ElectorateID electorateID);
    }
    
    private class LeaderWatch implements Watcher
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void process(WatchedEvent event)
        {
            if (event.getState() == KeeperState.Disconnected) {
                myListener.notifyLeaderDown(getID());
            }
        }
    }
    
    private final ElectorateID myID;
    
    private final ZooKeeperClient myZKClient;
    
    private final String myZKPath;
    
    private final Listener myListener;

    /**
     * CTOR
     */
    public Electorate(ElectorateID id, 
                      ZooKeeperClient zc,
                      Listener listener)
    {
        myID    = id;
        myZKClient = zc;
        myListener = listener;
        myZKPath = ZooKeeperClient.PATH_SEPARATOR + id.toString();
    }
    
    /**
     * Adds a node to the electorate. If the node is addeed as a leader 
     * it will acquire the lock. Else it will just add the node and add a 
     * watcher to the lock node.
     */
    public void add(NodeID node, boolean isLeader)
    {
        myZKClient.addNode(myID.toString(), node);
        if (isLeader) {
            myZKClient.acquireLockUnder(myZKPath, node);
        }
        else {
            if (myZKClient.isLockNodeAvailable(myZKPath)) {
                myZKClient.addWatcherToLockHolder(
                    myZKPath, new LeaderWatch());
            }
            else {
                myZKClient.addWatcherToLockHolder(
                    myZKPath,
                    new Watcher() {
                        
                        @Override
                        public void process(WatchedEvent event)
                        {
                            if (event.getState() == KeeperState.SyncConnected) 
                            {
                                myListener.notifyLeaderAvailability(getID());
                                myZKClient.addWatcherToLockHolder(
                                    myZKPath, new LeaderWatch());
                            }
                        }
                    });
            }
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
     * Returns the leader node and it's term id.
     */
    public Pair<NodeID,Long> getLeaderAndTermID()
    {
        
    }
}
