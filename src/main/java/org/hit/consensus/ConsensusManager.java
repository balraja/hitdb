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

package org.hit.consensus;

import java.util.HashMap;
import java.util.Map;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.event.ConsensusRequestEvent;
import org.hit.event.CreateConsensusAcceptorEvent;
import org.hit.event.CreateConsensusLeaderEvent;
import org.hit.event.Event;
import org.hit.event.ProposalNotificationResponse;
import org.hit.messages.ConsensusMessage;

import com.google.inject.Inject;

/**
 * Defines the contract for a service that's responsible for achieving
 * consensus among it's peers.
 * 
 * @author Balraja Subbiah
 */
public class ConsensusManager extends Actor
{
    private final Map<UnitID, ConsensusProtocol> myUnitToConsensusProtocolMap;
    
    private final NodeID                         myNodeID;
    
    /**
     * CTOR
     */
    @Inject
    public ConsensusManager(EventBus                  eventBus,
                            NodeID                    myID)
    {
        super(eventBus, new ActorID(ConsensusManager.class.getName()));
        myUnitToConsensusProtocolMap = new HashMap<>();
        myNodeID                     = myID;
    }
    
    private ConsensusProtocolProvider makeProvider(UnitID unitID)
    {
        return unitID.getConsensusType().makeProvider();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processEvent(Event event)
    {
        if (event instanceof CreateConsensusLeaderEvent) {
            CreateConsensusLeaderEvent ccle =
                (CreateConsensusLeaderEvent) event;
            myUnitToConsensusProtocolMap.put(
                ccle.getUnitID(),
                makeProvider(ccle.getUnitID()).makeLeader(
                    ccle.getUnitID(),
                    ccle.getAcceptors(),
                    getEventBus(),
                    myNodeID));
        }
        else if (event instanceof CreateConsensusAcceptorEvent) {
            CreateConsensusAcceptorEvent ccae =
                (CreateConsensusAcceptorEvent) event;
            
            myUnitToConsensusProtocolMap.put(
                ccae.getUnitID(),
                makeProvider(ccae.getUnitID()).makeAcceptor(
                    ccae.getUnitID(),
                    ccae.getLeader(),
                    myNodeID,
                    getEventBus()));
        }
        else if (event instanceof ConsensusRequestEvent) {
            ConsensusRequestEvent cre = (ConsensusRequestEvent) event;
            ConsensusLeader leader =
                (ConsensusLeader) myUnitToConsensusProtocolMap.get(
                    cre.getProposal().getUnitID());
            leader.getConsensus(cre.getProposal());
        }
        else if (event instanceof ProposalNotificationResponse) {
            ProposalNotificationResponse response = 
                (ProposalNotificationResponse) event;
            ConsensusAcceptor acceptor =
                (ConsensusAcceptor) myUnitToConsensusProtocolMap.get(
                    response.getProposalNotification()
                            .getProposal()
                            .getUnitID());
            acceptor.handleResponse(response);
        }
        else if (event instanceof ConsensusMessage) {
            ConsensusMessage message = (ConsensusMessage) event;
            ConsensusProtocol consensusProtocol =
                myUnitToConsensusProtocolMap.get(message.getUnitID());
            
            if (consensusProtocol == null) {
                // we are treating this case as the first instance of 
                // a consensus leader soliciting the response from the
                // acceptors and an acceptor for this consensus unit 
                // is not present. So we create one here.
                consensusProtocol = 
                    makeProvider(message.getUnitID()).makeAcceptor(
                        message.getUnitID(),
                        message.getSenderId(),
                        myNodeID,
                        getEventBus());
                
                myUnitToConsensusProtocolMap.put(message.getUnitID(),
                                                 consensusProtocol);
            }
            consensusProtocol.handle(message);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(CreateConsensusLeaderEvent.class,
                                       getActorID());
        getEventBus().registerForEvent(CreateConsensusAcceptorEvent.class,
                                       getActorID());
        getEventBus().registerForEvent(ConsensusRequestEvent.class,
                                       getActorID());
        getEventBus().registerForEvent(ConsensusMessage.class,
                                       getActorID());
        
    }
}
