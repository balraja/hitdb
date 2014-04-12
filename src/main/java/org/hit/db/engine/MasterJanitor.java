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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.db.model.HitTableSchema;
import org.hit.db.partitioner.TablePartitionInfo;
import org.hit.event.DBStatEvent;
import org.hit.event.Event;
import org.hit.event.SendMessageEvent;
import org.hit.messages.Allocation;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.CreateTableResponseMessage;
import org.hit.messages.FacadeInitRequest;
import org.hit.messages.FacadeInitResponse;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.NodeAdvertisementResponse;
import org.hit.server.ServerConfig;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;
import org.hit.util.Pair;

import com.google.inject.Inject;

/**
 * Defines the contract for module responsible for managing the nodes.
 *
 * @author Balraja Subbiah
 */
public class MasterJanitor extends AbstractJanitor
{
    private static final Logger LOG =
       LogFactory.getInstance().getLogger(MasterJanitor.class);

    private static final String TABLE_CREATION_FAILURE_MSG =
        "Creation of table %s has failed";

    private static final String TABLE_CREATION_LOG =
        "Received request from %s for creating table %s";

    private final Allocator myAllocator;

    private final ScheduledExecutorService myScheduler;
    
    private final Map<String, Pair<NodeID, Set<NodeID>>> myTableCreationState;

    /**
     * CTOR
     */
    @Inject
    public MasterJanitor(TransactionManager transactionManager,
                         ServerConfig engineConfig,
                         EventBus eventBus,
                         NodeID masterID,
                         Allocator allocator)
    {
        super(transactionManager, engineConfig, eventBus, masterID);
        myAllocator = allocator;
        myScheduler =
            Executors.newScheduledThreadPool(
                1,
                new NamedThreadFactory(MasterJanitor.class));
        ((ThreadPoolExecutor) myScheduler).prestartCoreThread();
        myTableCreationState = new HashMap<>();
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
                                       ctm.getSenderId(),
                                       ctm.getTableSchema()));
                HitTableSchema schema = ctm.getTableSchema();
                myAllocator.addSchema(schema);
                boolean isSuccess = 
                    getTransactionManager().createTable(schema);
                
                if (isSuccess) {
                    myTableCreationState.put(
                        schema.getTableName(), 
                        new Pair<NodeID, Set<NodeID>>(
                            ctm.getSenderId(),
                            new HashSet<>(myAllocator.getMonitoredNodes())));
                        
                    getEventBus().publish(
                        ActorID.DB_ENGINE, 
                         SendMessageEvent.create(
                            myAllocator.getMonitoredNodes(),
                            new CreateTableMessage(getServerID(), schema)));
                    

                    LOG.info(" Schema for " + schema.getTableName()
                             + " is sent to " + myAllocator.getMonitoredNodes());
                }
                else {
                    CreateTableResponseMessage response =
                        new CreateTableResponseMessage(
                            getServerID(),
                            schema.getTableName(),
                            null,
                            String.format(TABLE_CREATION_FAILURE_MSG,
                                          schema.getTableName()));

                    LOG.info("Schema addition for " + schema.getTableName()
                             + " failed");
                    
                    getEventBus().publish(
                            ActorID.DB_ENGINE,
                            SendMessageEvent.create(ctm.getSenderId(),
                                                    response));
                }
            }
            else if (event instanceof CreateTableResponseMessage) {
                
                CreateTableResponseMessage ctr = 
                    (CreateTableResponseMessage) event;
                
                LOG.info("Received response from " + ctr.getSenderId()
                        + " towards adding schema for " 
                        + ctr.getTableName());
                
                Pair<NodeID,Set<NodeID>> createTableState = 
                    myTableCreationState.get(ctr.getTableName());
                
                if (createTableState != null) {
                    
                    createTableState.getSecond().remove(ctr.getSenderId());
                    
                    if (createTableState.getSecond().isEmpty()) {
                        
                        CreateTableResponseMessage clientResponse = 
                            new CreateTableResponseMessage(
                                    getServerID(), 
                                    ctr.getTableName(),
                                    myAllocator.getPartitions()
                                               .get(ctr.getTableName()),
                                    null);
                        
                        getEventBus().publish(
                            ActorID.DB_ENGINE,
                            SendMessageEvent.create(createTableState.getFirst(),
                                                    clientResponse));
                    }
                }
                else {
                    LOG.severe("Received CreateTableResponse for "
                               + " create table request for creating "
                               + ctr.getTableName()
                               + " from "
                               + ctr.getSenderId()
                               + " which wasn't send by us ");
                }
            }
            else if (event instanceof NodeAdvertisement) {
                NodeAdvertisement na = (NodeAdvertisement) event;
                Allocation allocation =
                    myAllocator.getAllocation(na.getSenderId());
                if (allocation != null) {
                    getEventBus().publish(
                        ActorID.DB_ENGINE,
                        SendMessageEvent.create(
                             na.getSenderId(),
                             new NodeAdvertisementResponse(
                                getServerID(), allocation)));
                    getEventBus().publish(
                        ActorID.DB_ENGINE,
                        myAllocator.getGossipUpdates());
                }
            }
            else if (event instanceof FacadeInitRequest) {
                FacadeInitRequest fir = (FacadeInitRequest) event;
                getEventBus().publish(
                    ActorID.DB_ENGINE,
                    SendMessageEvent.create(
                        fir.getSenderId(),
                        new FacadeInitResponse(
                            getServerID(),
                            new TablePartitionInfo(
                                myAllocator.getPartitions()))));
            }
        }
        catch (IllegalAccessException e) {
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
        getEventBus().registerForEvent(CreateTableResponseMessage.class, actorID);
    }

    /**
     * Starts the master warden to enable it to process the events.
     */
    public void start(Set<NodeID> otherNodes)
    {
        super.start();
        
        myAllocator.initialize(otherNodes);
        getIsInitialized().set(true);
        
        LOG.info("Scheduling task to publish updates to gossiper every "
                + getServerConfig().getGossipUpdateSecs() + " seconds");
        
        myScheduler.scheduleAtFixedRate(
            new Runnable()
            {
                @Override
                public void run()
                {
                    if (getIsInitialized().get()) {
                        getEventBus().publish(
                            ActorID.DB_ENGINE, myAllocator.getGossipUpdates());
                    }
                }
            },
            getServerConfig().getGossipUpdateSecs(),
            getServerConfig().getGossipUpdateSecs(),
            TimeUnit.SECONDS);
        
       
        LOG.info("Started master warden");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        myScheduler.shutdownNow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleDbStats(DBStatEvent dbStats)
    {
        myAllocator.listenTO(dbStats);
    }
}
