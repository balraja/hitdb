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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.NodeID;
import org.hit.event.DBStatEvent;
import org.hit.event.Event;
import org.hit.event.GossipNotificationEvent;
import org.hit.event.SendMessageEvent;
import org.hit.gossip.Gossip;
import org.hit.messages.Heartbeat;
import org.hit.partitioner.Partitioner;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

import com.google.inject.Inject;

/**
 * Defines the <code>NodeCoordinator</code> that acts as client to the
 * <code>NodeMonitor</code> running on the master.
 *
 * @author Balraja Subbiah
 */
public class LocalWarden extends AbstractWarden
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
            try {
                if (myMaster != null) {
                    getEventBus().publish(
                        new SendMessageEvent(
                            Collections.singletonList(myMaster),
                            new Heartbeat(myTableRowCountMap)));
                }
            }
            catch (EventBusException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(LocalWarden.class);

    private NodeID myMaster;

    private final Map<String, Partitioner<?, ?>> myPartitions;

    private final ScheduledExecutorService myScheduler;

    private final TObjectLongMap<String> myTableRowCountMap;

    /**
     * CTOR
     */
    @Inject
    public LocalWarden(TransactionManager transactionManager,
                       EngineConfig       engineConfig,
                       EventBus           eventBus)
    {
        super(transactionManager, engineConfig, eventBus);
        myPartitions = new HashMap<>();
        myTableRowCountMap = new TObjectLongHashMap<>();
        myScheduler =
            Executors.newScheduledThreadPool(
                1,
                new NamedThreadFactory("NodeCoordinatorScheduler"));
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
                    myPartitions.put((String)              gossip.getKey(),
                                     (Partitioner<?,?>) gossip);
                }
            }
        }
        else if (event instanceof DBStatEvent) {
            DBStatEvent stat = (DBStatEvent) event;
            myScheduler.submit(new ApplyDBStatsTask(stat));
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
}
