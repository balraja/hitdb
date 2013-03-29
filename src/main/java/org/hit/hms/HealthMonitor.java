/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

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

package org.hit.hms;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.NodeID;
import org.hit.event.Event;
import org.hit.event.SendMessageEvent;
import org.hit.messages.GossipMessage;
import org.hit.topology.Topology;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

import com.google.inject.Inject;

/**
 * Defines the contract for a service that monitors the health of other nodes
 * participating in the distributed database. This uses gossip protocol for
 * monitoring the health of nodes in a system.
 * 
 * @author Balraja Subbiah
 */
public class HealthMonitor extends Actor
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HealthMonitor.class);
    
    private final Topology myTopology;
    
    private final SuspectMatrix mySuspectMatrix;
    
    private final ScheduledExecutorService myScheduler;
    
    private Future<?> myScheduledFuture;
    
    private final Random myRandom;
    
    private final NodeID myNodeID;
    
    /**
     * CTOR
     */
    @Inject
    public HealthMonitor(Topology topology, NodeID nodeID, EventBus eventBus)
    {
        super(eventBus, new ActorID(HealthMonitor.class.getSimpleName()));
        myTopology = topology;
        myNodeID = nodeID;
        mySuspectMatrix =
            new SuspectMatrix(myNodeID, new SuspectVector(myTopology));
        myScheduler =
            Executors.newScheduledThreadPool(1,
                                             new NamedThreadFactory(getClass())
            );
        myRandom = new Random();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof GossipMessage) {
            GossipMessage gm = (GossipMessage) event;
            mySuspectMatrix.addSuspectVector(gm.getNodeId(),
                                             gm.getSuspectVector());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(GossipMessage.class, getActorID());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        super.start();
        myScheduledFuture =
            myScheduler.schedule(
                new Runnable() {
                   /**
                    * {@inheritDoc}
                    */
                   @Override
                   public void run()
                   {
                       mySuspectMatrix.updateElapsedGossipCycles();
                       int nextDestination =
                           myRandom.nextInt(
                               myTopology.getParticipatingNodes().size())
                           % myTopology.getParticipatingNodes().size();
                       
                       NodeID target =
                           myTopology.getParticipatingNodes()
                                     .get(nextDestination);
                       
                       try {
                           getEventBus().publish(
                               new SendMessageEvent(
                                   Collections.singleton(target),
                                   new GossipMessage(
                                       myNodeID,
                                       mySuspectMatrix.getSuspectVector(
                                           myNodeID))
                                   )
                               );
                       }
                       catch (EventBusException e) {
                           LOG.log(Level.SEVERE,
                                   e.getMessage(),
                                   e);
                       }
                   }
                },
                GossipInformation.GOSSIP_INTERVAL_IN_SECONDS,
                TimeUnit.SECONDS);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        super.stop();
        if (myScheduledFuture != null) {
            myScheduledFuture.cancel(true);
        }
    }
}
