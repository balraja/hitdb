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
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.event.Event;
import org.hit.event.GossipNotificationEvent;
import org.hit.event.GossipUpdateEvent;
import org.hit.event.PeriodicTaskNotification;
import org.hit.event.PeriodicTaskScheduleRequest;
import org.hit.event.SendMessageEvent;
import org.hit.messages.NodeAdvertisement;
import org.hit.messages.ReconcillationRequest;
import org.hit.messages.ReconcilliationResponse;
import org.hit.util.LogFactory;

import com.google.inject.Inject;

/**
 * Defines the contract for a type that's responsible for disseminating
 * <code>Information</code> known to it to all other nodes via gossiping.
 * 
 * @author Balraja Subbiah
 */
public class Disseminator extends Actor
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(Disseminator.class);

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
                getEventBus().publish(
                        ActorID.GOSSIPER,
                        new SendMessageEvent(
                            Collections.singletonList(nodeID),
                            new ReconcillationRequest(myOwner, digest)));
               
            }
        }
    }
    
    private final List<NodeID> myParticipants;
    
    private final NodeID myOwner;
    
    private final Repository myRepository;
    
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof PeriodicTaskNotification) {
            PeriodicTaskNotification periodicTaskNotification =
                (PeriodicTaskNotification) event;
            periodicTaskNotification.getPeriodicTask().run();
        }
        if (event instanceof ReconcilliationResponse) {
            ReconcilliationResponse response = 
                (ReconcilliationResponse) event;
            myRepository.update(response.getInformationList());
            publish(new GossipNotificationEvent(
                myRepository.getLatestInformation()));
         
        }
        else if (event instanceof ReconcillationRequest) {
            ReconcillationRequest rr = (ReconcillationRequest) event;
            if (!myParticipants.contains(rr.getSenderId())) {
                LOG.info("Adding " + rr.getSenderId() + " to "
                         + myOwner + "'s gossip neighbours list");
                myParticipants.add(rr.getSenderId());
            }
            publish(
                new SendMessageEvent(
                    Collections.singletonList(rr.getSenderId()),
                    new ReconcilliationResponse(
                        myOwner,
                        myRepository.processDigest(rr.getDigest()))));
        }
        else if (event instanceof NodeAdvertisement) {
            NodeAdvertisement advertisement = (NodeAdvertisement) event;
            myParticipants.add(advertisement.getSenderId());
        }
        else if (event instanceof GossipUpdateEvent) {
            GossipUpdateEvent update = (GossipUpdateEvent) event;
            
            for (Gossip gossip : update.getGossip())
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
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        super.start();
        publish(new PeriodicTaskScheduleRequest(
             getActorID(), new SendDigestPeriodicTask(), 5, TimeUnit.MINUTES));
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
