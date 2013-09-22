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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.event.DBStatEvent;
import org.hit.event.Event;
import org.hit.event.GossipNotificationEvent;
import org.hit.event.SendMessageEvent;
import org.hit.gossip.Gossip;
import org.hit.messages.Allocation;
import org.hit.messages.DataLoadRequest;
import org.hit.messages.DataLoadResponse;
import org.hit.messages.Heartbeat;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.NodeAdvertisementResponse;
import org.hit.partitioner.Partitioner;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;

/**
 * Defines the <code>Coordinator</code> that acts as client to the
 * <code>Monitor</code> running on the master.
 *
 * @author Balraja Subbiah
 */
public class SlaveWarden extends AbstractWarden
{
    private class ApplyDBStatsTask implements Runnable
    {
        private final DBStatEvent myDBStat;

        /**
         * CTOR
         */
        public ApplyDBStatsTask(DBStatEvent stat)
        {
            myDBStat = stat;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            myTableRowCountMap.putAll(myDBStat.getTableToRowCountMap());
        }
    }

    private class PublishHeartbeatTask implements Runnable
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            if (myMaster != null) {
                getEventBus().publish(
                    new SendMessageEvent(
                        Collections.singletonList(myMaster),
                        new Heartbeat(myTableRowCountMap)));
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
        public void onFailure(Throwable arg0)
        {
            throw new RuntimeException(args0);
            
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onSuccess(DataLoadResponse response)
        {
            getTransactionManager().processOperation(
                response.getDataLoadResponseMutation());
            myAllocation.getTableToDataNodeMap().remove(
                response.getTableName());
            
            if (myAllocation.getTableToDataNodeMap().isEmpty()) {
                getIsInitialized().compareAndSet(false, true);
            }
            else {
                sendDataFetchRequest(myAllocation);
            }
        }
    }

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(SlaveWarden.class);

    private NodeID myMaster;

    private final Map<String, Partitioner<?, ?>> myPartitions;

    private final ListeningScheduledExecutorService myScheduler;

    private final TObjectLongMap<String> myTableRowCountMap;
    
    private final Map<NodeID, SettableFuture<DataLoadResponse>> myNodeToResponseFutureMap;
    
    /**
     * CTOR
     */
    @Inject
    public SlaveWarden(TransactionManager transactionManager,
                       EngineConfig       engineConfig,
                       EventBus           eventBus,
                       NodeID             slaveID)
    {
        super(transactionManager, engineConfig, eventBus, slaveID);
        myPartitions = new HashMap<>();
        myTableRowCountMap = new TObjectLongHashMap<>();
        myNodeToResponseFutureMap = new HashMap<>();
        
        myScheduler =
            MoreExecutors.listeningDecorator(
                Executors.newScheduledThreadPool(
                    1,
                    new NamedThreadFactory("NodeCoordinatorScheduler")));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleEvent(Event event)
    {
        super.handleEvent(event);
        if (event instanceof GossipNotificationEvent) {
            GossipNotificationEvent gne = (GossipNotificationEvent) event;
            for (Gossip gossip : gne.getGossip()) {
                if (gossip instanceof Partitioner) {
                    myPartitions.put((String)           gossip.getKey(),
                                     (Partitioner<?,?>) gossip);
                }
            }
        }
        else if (event instanceof DBStatEvent) {
            DBStatEvent stat = (DBStatEvent) event;
            myScheduler.submit(new ApplyDBStatsTask(stat));
        }
        else if (event instanceof NodeAdvertisementResponse) {
            NodeAdvertisementResponse nar = 
                (NodeAdvertisementResponse) event;
            if (nar.getAllocation() != null) {
                sendDataFetchRequest(nar.getAllocation());
            }
        }
        else if (event instanceof DataLoadResponse) {
            DataLoadResponse dlr = (DataLoadResponse) event;
            SettableFuture<DataLoadResponse> nodeFuture = 
                myNodeToResponseFutureMap.get(dlr.getSenderId());
            if (nodeFuture != null) {
                nodeFuture.set(dlr);
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
        getEventBus().registerForEvent(
            GossipNotificationEvent.class, actorID);
        getEventBus().registerForEvent(DBStatEvent.class, actorID);
        getEventBus().registerForEvent(NodeAdvertisementResponse.class, actorID);
        getEventBus().registerForEvent(DataLoadResponse.class, actorID);
    }

    /**
     * Setter for the master
     */
    public void setMaster(NodeID master)
    {
        myMaster = master;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        getEventBus().publish(
            new SendMessageEvent(Collections.singletonList(myMaster),
                                 new NodeAdvertisement(getServerID())));
        
        myScheduler.scheduleWithFixedDelay(
            new PublishHeartbeatTask(),
            getEngineConfig().getHeartBeatIntervalSecs(),
            getEngineConfig().getHeartBeatIntervalSecs(),
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
    
    private void sendDataFetchRequest(Allocation allocation)
    {
        String tableName = allocation.getTableToDataNodeMap()
                                      .keySet()
                                      .iterator()
                                      .next();
        NodeID targetNode = allocation.getTableToDataNodeMap()
                                      .get(tableName);
        
        getEventBus().publish(new SendMessageEvent(
            Collections.singletonList(targetNode),
            new DataLoadRequest(getServerID(), 
                                tableName,
                                allocation.getTable2PartitionMap()
                                          .get(tableName)
                                          .getNodeRange(getServerID()))));
        
        SettableFuture<DataLoadResponse> future = SettableFuture.create();
        Futures.addCallback(future, new LoadAllocationTask(allocation));
        myNodeToResponseFutureMap.put(targetNode, future);
    }
}
