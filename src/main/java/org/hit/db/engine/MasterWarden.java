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

package org.hit.db.engine;

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.NodeID;
import org.hit.event.DBStatEvent;
import org.hit.event.Event;
import org.hit.event.SchemaNotificationEvent;
import org.hit.event.SendMessageEvent;
import org.hit.facade.HitDBFacade;
import org.hit.messages.Allocation;
import org.hit.messages.FacadeInitRequest;
import org.hit.messages.FacadeInitResponse;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.NodeAdvertisementResponse;
import org.hit.util.LogFactory;

import com.google.inject.Inject;

/**
 * Defines the contract for module responsible for managing the nodes.
 *
 * @author Balraja Subbiah
 */
public class MasterWarden
{
    private static final Logger LOG =
       LogFactory.getInstance().getLogger(HitDBFacade.class);

    private final Allocator myAllocator;

    private final NodeID myNodeID;

    /**
     * CTOR
     */
    @Inject
    public MasterWarden(EventBus eventBus,
                       EngineConfig config,
                       NodeID nodeID,
                       Allocator allocator)
    {
        super(eventBus, new ActorID(MasterWarden.class.getSimpleName()));
        myNodeID = nodeID;
        myAllocator = allocator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        try {
            if (event instanceof SchemaNotificationEvent) {
                SchemaNotificationEvent sne =
                    (SchemaNotificationEvent) event;
                myAllocator.addSchema(sne.getSchema());
            }
            else if (event instanceof DBStatEvent) {
                DBStatEvent stat = (DBStatEvent) event;
                myAllocator.listenTO(stat);
            }
            else if (event instanceof NodeAdvertisement) {
                NodeAdvertisement na = (NodeAdvertisement) event;
                Allocation allocation = myAllocator.getAllocation(na.getNodeId());
                if (allocation != null) {
                    getEventBus().publish(
                        new SendMessageEvent(
                            Collections.singletonList(na.getNodeId()),
                            new NodeAdvertisementResponse(myNodeID, allocation)));
                    getEventBus().publish(myAllocator.getGossipUpdates());
                }

            }
            else if (event instanceof FacadeInitRequest) {
                FacadeInitRequest fir = (FacadeInitRequest) event;
                myFacadeAddresses.add(fir.getNodeId());
                getEventBus().publish(
                      new SendMessageEvent(
                          Collections.singletonList(fir.getNodeId()),
                          new FacadeInitResponse(myNodeID,
                                                 myAllocator.getPartitions())));
            }
        }
        catch (IllegalAccessException | EventBusException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(
            NodeAdvertisement.class, getActorID());
        getEventBus().registerForEvent(DBStatEvent.class, getActorID());
        getEventBus().registerForEvent(SchemaNotificationEvent.class,
                                       getActorID());
        getEventBus().registerForEvent(FacadeInitRequest.class,
                                       getActorID());
    }
}
