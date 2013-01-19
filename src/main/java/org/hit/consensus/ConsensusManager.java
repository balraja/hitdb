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
import org.hit.event.ConsensusMessage;
import org.hit.event.ConsensusRequestEvent;
import org.hit.event.CreateConsensusAcceptorEvent;
import org.hit.event.CreateConsensusLeaderEvent;
import org.hit.event.Event;
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
    
    private final ConsensusProtocolProvider      myProvider;
    
    private final NodeID                         myNodeID;
    
    /**
     * CTOR
     */
    @Inject
    public ConsensusManager(EventBus                  eventBus,
                            ConsensusProtocolProvider provider,
                            NodeID                    myID)
    {
        super(eventBus, new ActorID(ConsensusManager.class.getName()));
        myUnitToConsensusProtocolMap = new HashMap<>();
        myProvider                   = provider;
        myNodeID                     = myID;
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
                myProvider.makeLeader(ccle.getUnitID(),
                                      ccle.getAcceptors(),
                                      getEventBus(),
                                      myNodeID));
        }
        else if (event instanceof CreateConsensusAcceptorEvent) {
            CreateConsensusAcceptorEvent ccae =
                (CreateConsensusAcceptorEvent) event;
            myUnitToConsensusProtocolMap.put(
                ccae.getUnitID(),
                myProvider.makeAcceptor(ccae.getUnitID(),
                                        ccae.getLeader(),
                                        myNodeID));
        }
        else if (event instanceof ConsensusRequestEvent) {
            ConsensusRequestEvent cre = (ConsensusRequestEvent) event;
            ConsensusLeader leader =
                (ConsensusLeader) myUnitToConsensusProtocolMap.get(
                    cre.getProposal().getUnitID());
            leader.getConsensus(cre.getProposal());
        }
        else if (event instanceof ConsensusMessage) {
            ConsensusMessage message = (ConsensusMessage) event;
            ConsensusProtocol consensusProtocol =
                myUnitToConsensusProtocolMap.get(message.getUnitID());
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
