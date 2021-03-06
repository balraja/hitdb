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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.hit.communicator.NodeID;
import org.hit.util.LogFactory;
import org.hit.util.Pair;
import org.hit.zookeeper.ZooKeeperClient;

/**
 * Defines the set nodes that participates in a election. This works as follows.
 * 
 * <p>
 * Under zookeeper, we create a node by name
 * </br>
 * <code>/group_name</code>
 * 
 * @author Balraja Subbiah
 */
public class Group
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(Group.class);
    
    private static final int ONLY_LEADER_CONNECTED_COUNT = 1;
                    
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
        private final int myExpectedGroupSize;
        
        
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
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Received event " + event.getType() 
                         + " while waiting for the participants " 
                         + " of group " + myID.toString()
                         + " connect ");
            }

            if (   event.getState() == KeeperState.SyncConnected
                && event.getType()  == EventType.NodeChildrenChanged)
            {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("The watch on " + event.getPath() + " is triggerred");
                }
                
                if (myZKClient.checkLockParticipantsCount(
                        myZKPath, myExpectedGroupSize, this))
                {
                    notifyGroupReady();
                }
            }
        }
    }
    
    private class WaitForLeaderWatch implements Watcher
    {
        private final NodeID myNodeID;
        
        private final int myExpectedGroupSize;
        
        /**
         * CTOR
         */
        public WaitForLeaderWatch(NodeID nodeID, int expectedGroupSize)
        {
            super();
            myNodeID           = nodeID;
            myExpectedGroupSize = expectedGroupSize;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void process(WatchedEvent event)
        {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Received event " + event.getType() 
                         + " while waiting for the leader " 
                         + " of group " + myID.toString()
                         + " connect ");
            }

            if (   event.getState() == KeeperState.SyncConnected
                && event.getType()  == EventType.NodeChildrenChanged)
            {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("The watch on " + event.getPath() 
                             + " is triggerred");
                }
                
                // Now a leader has connected, so add this node to the
                // participants list under the group and wait for other
                // members to be connected.
                myZKClient.addNodeAsLockContender(myZKPath, myNodeID);

                if (myZKClient.checkLockParticipantsCount(
                        myZKPath, myExpectedGroupSize))
                {
                    notifyGroupReady();
                }
                else {
                    myZKClient.addWatchToLockNode(
                        myZKPath, new WaitForMembersWatch(myExpectedGroupSize));
                }
            }
        }
    }
    
    private final GroupID myID;
    
    private final ZooKeeperClient myZKClient;
    
    private final String myZKPath;
    
    private final Listener myListener;
    
    private final AtomicBoolean myGroupsReady;

    /**
     * CTOR
     */
    public Group(GroupID id, 
                 Listener listener,
                 ZooKeeperClient zkClient)
    {
        myID    = id;
        myZKClient = zkClient;
        myListener = listener;
        myZKPath = ZooKeeperClient.PATH_SEPARATOR + id.toString().toLowerCase();
        myGroupsReady = new AtomicBoolean(false);
    }
    
    /**
     * Adds a node to the group. If the node is added as a leader 
     * it will acquire the lock. Else it will just add the node and add a 
     * watcher to the lock node.
     */
    public void initGroup(NodeID node, boolean isLeader, int expectedSize)
    {
        LOG.info("Adding " + node + " to group " + myID + " as leader "
                 + isLeader + " with expected group size " + expectedSize);
        
        myZKClient.addNode(myZKPath, node);
        
        if (isLeader) {
            boolean isLocked = 
                myZKClient.acquireLockUnder(myZKPath, node);
            
            if (isLocked) {
                LOG.info("The " + node + " acquired lock under " + myID);
            }
            
            if (myZKClient.checkLockParticipantsCount(
                    myZKPath, 
                    expectedSize, 
                    new WaitForMembersWatch(expectedSize))) 
            {
                notifyGroupReady();
            }
        }
        else {
            boolean isLockAcquiredAlready = 
                myZKClient.checkLockParticipantsCount(
                    myZKPath, 
                    ONLY_LEADER_CONNECTED_COUNT,
                    new WaitForLeaderWatch(node, expectedSize));
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("IS LOCK ACQUIRED FOR " + myID + " " 
                         + isLockAcquiredAlready);
            }
            
            if (isLockAcquiredAlready) {
                myZKClient.addNodeAsLockContender(myZKPath, node);
                
                boolean isAllNodesAvailable = 
                    myZKClient.checkLockParticipantsCount(
                        myZKPath, 
                        expectedSize,
                        new WaitForMembersWatch(expectedSize));
                
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("IS ALL NODES AVAILABLE FOR " + myID + " " 
                             + isAllNodesAvailable);
                }

                if (isAllNodesAvailable) {
                    notifyGroupReady();
                    myZKClient.addWatchToLockHolder(myZKPath, 
                                                    new LeaderWatch());
                }
            }
            
        }
    }
    
    public void selectLeader(NodeID thisServerID)
    {
        myZKClient.acquireLockUnder(myZKPath, thisServerID);
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
        if (!myGroupsReady.compareAndSet(false, true)) {
            return;
        }
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Notifying group ready for " + myID);
        }

        List<NodeID> groupNodes = myZKClient.getNodes(myZKPath);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("The nodes under group " + myID + " are " + groupNodes);
        }

        Pair<NodeID,Long> leader = myZKClient.getLockHolder(myZKPath);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("The leader for group " + myID + " is " + leader.getFirst());
        }

        groupNodes.remove(leader.getFirst());
        myListener.notifyGroupReady(myID, 
                                    leader.getSecond(), 
                                    leader.getFirst(),
                                    groupNodes);
    }
}
