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

import com.google.inject.Inject;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.event.Event;
import org.hit.event.GroupReadyEvent;
import org.hit.event.JoinGroupEvent;
import org.hit.util.LogFactory;
import org.hit.zookeeper.ZooKeeperClient;

/**
 * Defines the contract for ElectionManager that helps in selecting a 
 * leader among a group of nodes.
 * 
 * @author Balraja Subbiah
 */
public class GroupManager extends Actor
    implements Group.Listener
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(GroupManager.class);
                    
    private final Map<GroupID, Group> myGroupMap;
    
    private final NodeID myServerID;
    
    private final ZooKeeperClient myZooKeeperClient;
    
    /**
     * CTOR
     */
    @Inject
    public GroupManager(EventBus        eventBus, 
                        NodeID          serverID,
                        ZooKeeperClient zkClient)
    {
        super(eventBus, ActorID.GROUP_MANAGER);
        myGroupMap = new HashMap<>();
        myServerID = serverID;
        myZooKeeperClient = zkClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof JoinGroupEvent) {
            JoinGroupEvent jgEvent = (JoinGroupEvent) event;
            LOG.info("Received event to join group " + jgEvent.getID());
            Group group = myGroupMap.get(jgEvent.getID());
            if (group == null) {
                group = 
                    new Group(jgEvent.getID(), this, myZooKeeperClient);
                
                myGroupMap.put(jgEvent.getID(), group);
                group.initGroup(
                    myServerID, 
                    jgEvent.isLeader(),
                    jgEvent.getExpectedSize());
            }
        }
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(JoinGroupEvent.class,
                                       getActorID());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyLeaderDown(GroupID groupID)
    {
        Group group = myGroupMap.get(groupID);
        group.selectLeader(myServerID);
        // Check and fix sending response of leader election.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void notifyGroupReady(GroupID groupID, 
                                 long term, 
                                 NodeID leader, 
                                 Collection<NodeID> followers)
    {
        LOG.info("Notifying group ready for " + groupID 
                 + " This group has " + leader + " as leader "
                 + " and " + followers  + " as followers");
        
        publish(new GroupReadyEvent(
            groupID, term, leader, new HashSet<NodeID>(followers)));
        
    }
}
