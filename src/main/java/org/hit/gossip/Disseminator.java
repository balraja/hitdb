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

package org.hit.gossip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.event.Event;
import org.hit.event.GossipNotificationEvent;
import org.hit.event.GossipUpdateEvent;
import org.hit.event.SendMessageEvent;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.ReconcillationRequest;
import org.hit.messages.ReconcilliationResponse;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

import com.google.inject.Inject;

/**
 * Defines the contract for a manager that's responsible for disseminating
 * <code>Information</code> known to it to all other nodes.
 * 
 * @author Balraja Subbiah
 */
public class Disseminator extends Actor
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(Disseminator.class);

    private class SendDigestTask implements Runnable
    {
        private final NodeID myParticipatingNode;
        
        private final NodeID myOwner;
        
        private final Digest myDigest;
        
        /**
         * CTOR
         */
        public SendDigestTask(NodeID participatingNode, 
                              NodeID owner,
                              Digest digest)
        {
            myParticipatingNode = participatingNode;
            myOwner = owner;
            myDigest = digest;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            getEventBus().publish(
                ActorID.GOSSIPER,
                new SendMessageEvent(
                    Collections.singletonList(myParticipatingNode),
                    new ReconcillationRequest(myOwner, myDigest)));
        }
    }
    
    private class ApplyDigestTask implements Runnable
    {
        private final NodeID myDigestOwner;
        
        private final Digest myDigest;
        
        /**
         * CTOR
         */
        public ApplyDigestTask(NodeID owner, Digest digest)
        {
            myDigestOwner = owner;
            myDigest = digest;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            if (!myParticipants.contains(myDigestOwner)) {
                myParticipants.add(myDigestOwner);
            }
            publish(
                new SendMessageEvent(
                    Collections.singletonList(myDigestOwner),
                    new ReconcilliationResponse(
                        myOwner,
                        myRepository.processDigest(myDigest))));
        }
    }

    
    private class ReconcillationResponseTask implements Runnable
    {
        private final List<Gossip> myResponse;
        
        /**
         * CTOR
         */
        public ReconcillationResponseTask(List<Gossip> response)
        {
            myResponse = response;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            myRepository.update(myResponse);
            publish(new GossipNotificationEvent(
                myRepository.getLatestInformation()));
        }
    }
    
    private class SendDigestPeriodicTask implements Runnable
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            Digest digest = myRepository.makeDigest();
            for (NodeID nodeID : myParticipants) {
                myScheduler.submit(
                    new SendDigestTask(nodeID, myOwner, digest));
            }
        }
    }
    
    private class AddParticipantTask implements Runnable
    {
        private final NodeID myNodeID;

        /**
         * CTOR
         */
        public AddParticipantTask(NodeID nodeID)
        {
            super();
            myNodeID = nodeID;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            myParticipants.add(myNodeID);
        }
    }
    
    private class GossipUpdateTask implements Runnable
    {
        private final GossipUpdateEvent myUpdateEvent;

        /**
         * CTOR
         */
        public GossipUpdateTask(GossipUpdateEvent updateEvent)
        {
            super();
            myUpdateEvent = updateEvent;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            for (Gossip gossip : myUpdateEvent.getGossip())
            {
                Gossip old = myRepository.lookup(gossip.getKey());
                if (old != null
                    && old.getTimestamp() < gossip.getTimestamp()) 
                {
                    myRepository.update(gossip);
                }
                else if (old == null){
                    myRepository.update(gossip);
                }
            }
        }
    }
    
    private final List<NodeID> myParticipants;
    
    private final NodeID myOwner;
    
    private final Repository myRepository;
    
    private final ScheduledExecutorService myScheduler;
    
    /**
     * CTOR
     */
    @Inject
    public Disseminator(EventBus eventBus, NodeID node)
    {
        super(eventBus, ActorID.GOSSIPER);
        myParticipants = new ArrayList<>();
        myOwner = node;
        myRepository = new Repository();
        myScheduler = 
            Executors.newScheduledThreadPool(
                1, new NamedThreadFactory("Disseminator Worker "));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof ReconcilliationResponse) {
            ReconcilliationResponse response = 
                (ReconcilliationResponse) event;
            myScheduler.submit(
                new ReconcillationResponseTask(
                    response.getInformationList()));
        }
        else if (event instanceof ReconcillationRequest) {
            ReconcillationRequest rr = (ReconcillationRequest) event;
            myScheduler.submit(new ApplyDigestTask(
                myOwner, rr.getDigest()));
        }
        else if (event instanceof NodeAdvertisement) {
            NodeAdvertisement advertisement = 
                (NodeAdvertisement) event;
            myScheduler.submit(
                new AddParticipantTask(advertisement.getSenderId()));
        }
        else if (event instanceof GossipUpdateEvent) {
            GossipUpdateEvent update = 
                (GossipUpdateEvent) event;
            myScheduler.submit(new GossipUpdateTask(update));
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        super.start();
        myScheduler.scheduleAtFixedRate(
            new SendDigestPeriodicTask(), 1, 1, TimeUnit.SECONDS);
    }
    

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(ReconcilliationResponse.class, 
                                       getActorID());
        
        getEventBus().registerForEvent(NodeAdvertisement.class,
                                       getActorID());
        
        getEventBus().registerForEvent(ReconcillationRequest.class,
                                       getActorID());
        
        getEventBus().registerForEvent(GossipUpdateEvent.class, 
                                       getActorID());
    }
}
