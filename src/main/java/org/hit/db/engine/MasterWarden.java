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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.NodeID;
import org.hit.db.model.Schema;
import org.hit.event.DBStatEvent;
import org.hit.event.Event;
import org.hit.event.SchemaNotificationEvent;
import org.hit.event.SendMessageEvent;
import org.hit.messages.Allocation;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.CreateTableResponseMessage;
import org.hit.messages.FacadeInitRequest;
import org.hit.messages.FacadeInitResponse;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.NodeAdvertisementResponse;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

import com.google.inject.Inject;

/**
 * Defines the contract for module responsible for managing the nodes.
 *
 * @author Balraja Subbiah
 */
public class MasterWarden extends AbstractWarden
{
    private static final Logger LOG =
       LogFactory.getInstance().getLogger(MasterWarden.class);
    
    private static final String TABLE_CREATION_LOG =
        "Received request from %s for creating table %s";
    
    private static final String TABLE_CREATION_FAILURE_MSG =
        "Creation of table %s has failed";

    private final Allocator myAllocator;

    private final NodeID myNodeID;
    
    private final ScheduledExecutorService myScheduler;
    
    /**
     * CTOR
     */
    @Inject
    public MasterWarden(TransactionManager transactionManager,
                        EngineConfig engineConfig,
                        EventBus eventBus,
                        NodeID nodeID,
                        Allocator allocator)
    {
        super(transactionManager, engineConfig, eventBus);
        myNodeID = nodeID;
        myAllocator = allocator;
        myScheduler = 
            Executors.newScheduledThreadPool(
                1,
                new NamedThreadFactory(MasterWarden.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleEvent(Event event)
    {
        super.handleEvent(event);
        try {
            if (event instanceof CreateTableMessage) {
                CreateTableMessage ctm = (CreateTableMessage) event;
                LOG.info(String.format(TABLE_CREATION_LOG,
                                       ctm.getNodeId(),
                                       ctm.getTableSchema()));
                Schema schema = ctm.getTableSchema();
                myAllocator.addSchema(schema);
                boolean isSuccess = 
                    getTransactionManager().createTable(schema);
                CreateTableResponseMessage response = null;
                if (isSuccess) {
                    response = 
                        new CreateTableResponseMessage(
                            myNodeID,
                            schema.getTableName(), 
                            myAllocator.getPartitions().get(
                                schema.getTableName()), 
                            null);
                    
                    LOG.info("Schema for " + schema.getTableName() 
                             + " was successfully"
                             + " added to the database");
                }
                else {
                    response = 
                        new CreateTableResponseMessage(
                            myNodeID, 
                            schema.getTableName(),
                            null, 
                            String.format(TABLE_CREATION_FAILURE_MSG, 
                                          schema.getTableName()));
                }
                getEventBus().publish(new SendMessageEvent(
                    Collections.singleton(myNodeID),
                    response));
            }
            else if (event instanceof DBStatEvent) {
                DBStatEvent stat = (DBStatEvent) event;
                myAllocator.listenTO(stat);
            }
            else if (event instanceof NodeAdvertisement) {
                NodeAdvertisement na = (NodeAdvertisement) event;
                Allocation allocation = 
                    myAllocator.getAllocation(na.getNodeId());
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
    public void register(ActorID actorID)
    {
        super.register(actorID);
        getEventBus().registerForEvent(
            NodeAdvertisement.class, actorID);
        getEventBus().registerForEvent(DBStatEvent.class, actorID);
        
        getEventBus().registerForEvent(FacadeInitRequest.class,
                                       actorID);
        getEventBus().registerForEvent(CreateTableMessage.class, actorID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        myScheduler.schedule(
            new Runnable()
            {
                /**
                 * {@inheritDoc}
                 */
                @Override
                public void run()
                {
                    try {
                        getEventBus().publish(
                            myAllocator.getGossipUpdates());
                    }
                    catch (EventBusException e) {
                        LOG.log(Level.SEVERE, e.getMessage(), e);
                    }
                }
            },
            getEngineConfig().getGossipUpdateSecs(),
            TimeUnit.SECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        myScheduler.shutdownNow();
    }
}
