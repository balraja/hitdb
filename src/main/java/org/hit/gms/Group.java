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
package org.hit.gms;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.hit.communicator.NodeID;
import org.hit.util.Pair;
import org.hit.zookeeper.ZooKeeperClient;

/**
 * Defines the set nodes that participates in a election.
 * 
 * @author Balraja Subbiah
 */
public class Group
{
    /**
     * A listener to be used for notifying about events
     * of interest wrt the group.
     */
    public static interface Listener
    {
        /**
         * Notifies that leader is down.
         */
        public void notifyLeaderDown(GroupID groupID);
        
        /**
         * Notifies that group is ready for processing now.
         */
        public void notifyGroupReady(GroupID groupID, 
                                     long term,
                                     NodeID leader,
                                     Collection<NodeID> followers);
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
    
    private class WaitForMembersWatch implements Watcher
    {
        private int myExpectedGroupSize;
        
        
        /**
         * CTOR
         */
        public WaitForMembersWatch(int expectedGroupSize)
        {
            super();
            myExpectedGroupSize = expectedGroupSize;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void process(WatchedEvent event)
        {
            if (   event.getState() == KeeperState.SyncConnected
                && event.getType()  == EventType.NodeChildrenChanged)
            {
                if (myZKClient.checkChildrenCount(myZKPath,
                                                  myExpectedGroupSize,
                                                  true))
                {
                    notifyGroupReady();
                }
                else {
                    myZKClient.addChildWatchToLockNode(myZKPath, 
                                                       this);
                }
            }
        }
        
    }
    
    private class WaitForLeaderWatch implements Watcher
    {
        private int myExpectedGroupSize;
        
        /**
         * CTOR
         */
        public WaitForLeaderWatch(int expectedGroupSize)
        {
            super();
            myExpectedGroupSize = expectedGroupSize;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void process(WatchedEvent event)
        {
            if (   event.getState() == KeeperState.SyncConnected
                && event.getType()  == EventType.NodeChildrenChanged)
            {
                if (myZKClient.checkChildrenCount(myZKPath,
                                                  myExpectedGroupSize,
                                                  true))
                {
                    notifyGroupReady();
                }
                else {
                    myZKClient.addChildWatchToLockNode(
                        myZKPath, new WaitForMembersWatch(myExpectedGroupSize));
                }
            }
        }
    }
    
    private final GroupID myID;
    
    private final ZooKeeperClient myZKClient;
    
    private final String myZKPath;
    
    private final Listener myListener;

    /**
     * CTOR
     */
    public Group(GroupID id, 
                      ZooKeeperClient zc,
                      Listener listener)
    {
        myID    = id;
        myZKClient = zc;
        myListener = listener;
        myZKPath = ZooKeeperClient.PATH_SEPARATOR + id.toString();
    }
    
    /**
     * Adds a node to the group. If the node is added as a leader 
     * it will acquire the lock. Else it will just add the node and add a 
     * watcher to the lock node.
     */
    public void initGroup(NodeID node, boolean isLeader, int expectedSize)
    {
        myZKClient.addNode(myID.toString(), node);
        if (isLeader) {
            myZKClient.acquireLockUnder(myZKPath, node);
            if (myZKClient.checkChildrenCount(myZKPath, expectedSize, true))
            {
                notifyGroupReady();
            }
            else {
                myZKClient.addChildWatchToLockNode(
                    myZKPath, new WaitForMembersWatch(expectedSize));
            }
        }
        else {
            boolean isLockAvailable = myZKClient.isLockNodeAvailable(myZKPath);
            boolean isAllNodesAvailable = 
                myZKClient.checkChildrenCount(myZKPath,
                                              expectedSize,
                                              true);
            
            if (isLockAvailable && isAllNodesAvailable) {
                notifyGroupReady();
            }
            else if (!isLockAvailable) {
                myZKClient.addChildWatchToLockNode(
                    myZKPath, new WaitForLeaderWatch(expectedSize));
            }
            else if (isLockAvailable && !isAllNodesAvailable) {
                myZKClient.addWatchToLockHolder(
                    myZKPath, new LeaderWatch());
            }
        }
    }

    /**
     * Returns the {@link GroupID} for this group.
     */
    public GroupID getID()
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
    
    private void notifyGroupReady()
    {
        myZKClient.addWatchToLockHolder(myZKPath, new LeaderWatch());
        
        List<NodeID> groupNodes = myZKClient.getNodes(myZKPath);
        Pair<NodeID,Long> leader = myZKClient.getLockHolder(myZKPath);
        groupNodes.remove(leader.getFirst());
        myListener.notifyGroupReady(myID, 
                                    leader.getSecond(), 
                                    leader.getFirst(),
                                    groupNodes);
    }
}
