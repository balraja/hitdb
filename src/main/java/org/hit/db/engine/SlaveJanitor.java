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

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.concurrent.pool.PooledObjects;
import org.hit.db.partitioner.Partitioner;
import org.hit.event.DBStatEvent;
import org.hit.event.Event;
import org.hit.event.GossipNotificationEvent;
import org.hit.event.PeriodicTaskScheduleRequest;
import org.hit.event.SendMessageEvent;
import org.hit.gossip.Gossip;
import org.hit.messages.Allocation;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.CreateTableResponseMessage;
import org.hit.messages.DataLoadRequest;
import org.hit.messages.DataLoadResponse;
import org.hit.messages.Heartbeat;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.NodeAdvertisementResponse;
import org.hit.server.ServerConfig;
import org.hit.util.LogFactory;
import org.hit.util.Range;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;

/**
 * Defines the <code>Coordinator</code> that acts as client to the
 * <code>Monitor</code> running on the master.
 *
 * @author Balraja Subbiah
 */
public class SlaveJanitor extends AbstractJanitor
{

    private class PublishHeartbeatTask implements Runnable
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            if (myMaster != null && getIsInitialized().get()) {
                getEventBus().publish(
                    ActorID.DB_ENGINE,
                    PooledObjects.getInstance(SendMessageEvent.class).initialize(
                        myMaster,
                        new Heartbeat(getServerID(), myTableRowCountMap)));
            }
        }
    }
    
    private class LoadAllocationTask implements FutureCallback<DataLoadResponse>
    {
        private final Allocation myAllocation;

        /**
         * CTOR
         */
        public LoadAllocationTask(Allocation allocation)
        {
            super();
            myAllocation = allocation;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onFailure(Throwable e)
        {
            throw new RuntimeException(e);
            
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(DataLoadResponse response)
        {
            getTransactionManager().createTable(
                myAllocation.getTable2SchemaMap().get(
                    response.getTableName()));
            
            getTransactionManager().processOperation(
                response.getDataLoadResponseMutation());
            
            myAllocation.getTableToDataNodeMap().remove(
                response.getTableName());
            
            if (myAllocation.getTableToDataNodeMap().isEmpty()) {
                getIsInitialized().compareAndSet(false, true);
                LOG.info(
                    "Successfully loaded data allocated data for all tables");
            }
            else {
                sendDataFetchRequest(myAllocation);
            }
        }
    }

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(SlaveJanitor.class);

    private NodeID myMaster;

    private final Map<String, Partitioner<?, ?>> myPartitions;

    private final TObjectLongMap<String> myTableRowCountMap;
    
    private final Map<NodeID, SettableFuture<DataLoadResponse>> 
        myNodeToResponseFutureMap;
    
    private Allocation myAllocation;
    
    /**
     * CTOR
     */
    @Inject
    public SlaveJanitor(TransactionManager transactionManager,
                        ServerConfig       serverConfig,
                        EventBus           eventBus,
                        NodeID             slaveID)
    {
        super(transactionManager, serverConfig, eventBus, slaveID);
        myPartitions = new HashMap<>();
        myTableRowCountMap = new TObjectLongHashMap<>();
        myNodeToResponseFutureMap = new HashMap<>();
        myAllocation = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleEvent(Event event)
    {
        // XXX Should we cache messages till the data is loaded from other 
        // nodes. Probably notify master also.
        super.handleEvent(event);
        if (event instanceof GossipNotificationEvent) {
            GossipNotificationEvent gne = (GossipNotificationEvent) event;
            for (Gossip gossip : gne.getGossip()) {
                if (gossip instanceof Partitioner) {
                    myPartitions.put((String)           gossip.getKey(),
                                     (Partitioner<?,?>) gossip);
                }
            }
            PooledObjects.freeInstance(gne);
        }
        else if (event instanceof CreateTableMessage) {
            
            CreateTableMessage ctm = (CreateTableMessage) event;
            // XXX Handle failure scenario also.
            getTransactionManager().createTable(ctm.getTableSchema());
            LOG.info("Notifying " + ctm.getSenderId() + " about "
                     + " the response for adding schema  " 
                     + ctm.getTableSchema());
            getEventBus().publish(
                    ActorID.DB_ENGINE,
                    PooledObjects.getInstance(SendMessageEvent.class).initialize(
                        ctm.getSenderId(),
                        new CreateTableResponseMessage(
                            getServerID(),
                            ctm.getTableSchema().getTableName(),
                            null,
                            null)));
            
        }
        else if (event instanceof NodeAdvertisementResponse) {
            NodeAdvertisementResponse nar = 
                (NodeAdvertisementResponse) event;
            if (nar.getAllocation() != null) {
                myAllocation = nar.getAllocation();
                sendDataFetchRequest(myAllocation);
            }
        }
        else if (event instanceof DataLoadResponse) {
            DataLoadResponse response = (DataLoadResponse) event;
            if (myAllocation != null) {
                getTransactionManager().createTable(
                        myAllocation.getTable2SchemaMap().get(
                            response.getTableName()));
                
                getTransactionManager().processOperation(
                    response.getDataLoadResponseMutation());
                
                myAllocation.getTableToDataNodeMap().remove(
                    response.getTableName());
                
                if (myAllocation.getTableToDataNodeMap().isEmpty()) {
                    getIsInitialized().compareAndSet(false, true);
                    LOG.info(
                        "Successfully loaded data allocated data for all tables");
                }
                else {
                    sendDataFetchRequest(myAllocation);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(ActorID actorID)
    {
        super.register(actorID);
        getEventBus().registerForEvent(GossipNotificationEvent.class, actorID);
        getEventBus().registerForEvent(DBStatEvent.class, actorID);
        getEventBus().registerForEvent(NodeAdvertisementResponse.class, actorID);
        getEventBus().registerForEvent(DataLoadResponse.class, actorID);
        getEventBus().registerForEvent(CreateTableMessage.class, actorID);
    }

    /**
     * Setter for the master
     */
    public void setMaster(NodeID master)
    {
        myMaster = master;
    }

    /**
     * Starts the slave warden with the master node.
     */
    public void start(NodeID master)
    {
        super.start();
        myMaster = master;
        
        LOG.info("Notifying " + myMaster + " about this node ");
        
        getEventBus().publish(
            ActorID.DB_ENGINE,
            PooledObjects.getInstance(SendMessageEvent.class).initialize(
                myMaster, new NodeAdvertisement(getServerID())));
        
        LOG.info("Scheduling task to publish hearbeats every  "
                 + getServerConfig().getHeartBeatIntervalSecs() + " seconds");
        
        getEventBus().publish(
            ActorID.DB_ENGINE,
            ActorID.TIME_KEEPER,
            new PeriodicTaskScheduleRequest(
                ActorID.DB_ENGINE,
                new PublishHeartbeatTask(),
                getServerConfig().getHeartBeatIntervalSecs(),
                TimeUnit.SECONDS));
        
        LOG.info("Successfully started slave warden");
    }

    private void sendDataFetchRequest(Allocation allocation)
    {
        if (!allocation.shouldFetchDataFromOtherNodes()) {
            getIsInitialized().compareAndSet(false, true);
            LOG.info("Allocation sent from the master is empty. So we are "
                     + " marking the db server to be ready to process requests"
                    + " from clients");
            return;
        }
        
        String tableName = allocation.getTableToDataNodeMap()
                                      .keySet()
                                      .iterator()
                                      .next();
        NodeID targetNode = allocation.getTableToDataNodeMap()
                                      .get(tableName);
        Range<?> range = 
                allocation.getTable2PartitionMap()
                          .get(tableName)
                          .getNodeRange(getServerID());
                          
        LOG.info("Sending request to " + targetNode + " for fetching data "
                 + " corresponding to " + tableName + " in the range "
                 + range);
        
        getEventBus().publish(
            ActorID.DB_ENGINE,
            PooledObjects.getInstance(SendMessageEvent.class).initialize(
                targetNode,
                new DataLoadRequest(getServerID(), 
                                    tableName,
                                    range)));
        
        SettableFuture<DataLoadResponse> future = SettableFuture.create();
        Futures.addCallback(future, new LoadAllocationTask(allocation));
        myNodeToResponseFutureMap.put(targetNode, future);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleDbStats(DBStatEvent dbStats)
    {
        myTableRowCountMap.putAll(dbStats.getTableToRowCountMap());
        PooledObjects.freeInstance(dbStats);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
    }
}
