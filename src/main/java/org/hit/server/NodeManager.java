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

package org.hit.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.NodeID;
import org.hit.event.Event;
import org.hit.event.MasterDownEvent;
import org.hit.event.SchemaNotificationEvent;
import org.hit.event.SendMessageEvent;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.NodeAdvertisementResponse;
import org.hit.partitioner.Partitioner;

import com.google.inject.Inject;

/**
 * Defines the contract for module responsible for managing the nodes.
 * 
 * @author Balraja Subbiah
 */
public class NodeManager extends Actor
{
    private final ServerConfig myConfig;
    
    private final Map<String, Partitioner<?>> myTableToKeySpaceMap;
    
    private final KeyspaceAllocator myAllocator;
    
    private NodeID myNodeID;
    
    /**
     * CTOR
     */
    @Inject
    public NodeManager(EventBus eventBus, 
                       ServerConfig config,
                       NodeID nodeID,
                       KeyspaceAllocator allocator)
    {
        super(eventBus, new ActorID(NodeManager.class.getSimpleName()));
        myConfig = config;
        myNodeID = nodeID;
        myTableToKeySpaceMap = new HashMap<>();
        myAllocator = allocator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof SchemaNotificationEvent) {
            SchemaNotificationEvent sne = 
                (SchemaNotificationEvent) event;
            myAllocator.addSchema(sne.getSchema());
        }
        else if (event instanceof NodeAdvertisement) {
            NodeAdvertisement na = (NodeAdvertisement) event;
            Allocation allocation = myAllocator.getAllocation(na.getNodeId());
            try {
                getEventBus().publish(
                    new SendMessageEvent(
                        Collections.singletonList(na.getNodeId()),
                        new NodeAdvertisementResponse(myNodeID, allocation)));
            }
            catch (EventBusException e) {
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        if (myConfig.isMaster()) {
            getEventBus().registerForEvent(
                NodeAdvertisement.class, getActorID());
        }
        else {
            getEventBus().registerForEvent(
                MasterDownEvent.class, getActorID());
        }
    }
}
