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
package org.hit.consensus.twopc;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.event.ConsensusResponseEvent;
import org.hit.event.SendMessageEvent;
import org.hit.util.LogFactory;

/**
 * Defines the contract for <code>ConsensusLeader</code> that drives for 
 * consensus in a <code>ConsensusProtocol</code>
 * 
 * @author Balraja Subbiah
 */
public class TwoPCLeader extends ConsensusLeader
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(TwoPCLeader.class);
        
    /**
     * Class to keep track of book keeping information in the leader.
     */
    private class AcceptanceInfo
    {
        private final Set<NodeID> myAcceptors;
        
        private boolean myShouldCommit;

        /**
         * CTOR
         */
        public AcceptanceInfo(Set<NodeID> acceptors)
        {
            super();
            myAcceptors = acceptors;
            myShouldCommit = false;
        }
        
        public void handleAccept(ConsensusAcceptMessage accept)
        {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Received " + accept.isAccepted() + " from "
                        + accept.getSenderId() 
                        + " on " + accept.getProposal());
            }
            
            if (myAcceptors.remove(accept.getSenderId())) {
                myShouldCommit = accept.isAccepted();
                if (myAcceptors.isEmpty()) {
                    
                    if (LOG.isLoggable(Level.FINE)) {
                        LOG.fine("Sending the commit request as "
                                + myShouldCommit);
                    }
                    getEventBus().publish(
                        ActorID.CONSENSUS_MANAGER,
                        new SendMessageEvent(
                            getAcceptors(),
                            new CommitRequest(getNodeID(),
                                              getConsensusUnitID(),
                                              accept.getProposal(), 
                                              myShouldCommit)));
                    
                    getEventBus().publish(
                        ActorID.CONSENSUS_MANAGER,
                        new ConsensusResponseEvent(
                            accept.getProposal(), myShouldCommit));
                }
            }
        }
    }
    
    private final Map<Proposal, AcceptanceInfo> myProposalToAcceptanceInfo;

    /**
     * CTOR
     */
    public TwoPCLeader(
        UnitID consensusUnitID,
        Set<NodeID> acceptors,
        EventBus eventBus,
        NodeID myID)
    {
        super(consensusUnitID, acceptors, eventBus, myID);
        myProposalToAcceptanceInfo = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(Message message)
    {
        if (message instanceof ConsensusAcceptMessage) {
            ConsensusAcceptMessage cam = (ConsensusAcceptMessage) message;
            AcceptanceInfo info = 
                myProposalToAcceptanceInfo.get(cam.getProposal());
            info.handleAccept(cam);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getConsensus(Proposal proposal)
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Initiating consensus on proposal " + proposal); 
        }
        
        myProposalToAcceptanceInfo.put(proposal,
                                       new AcceptanceInfo(
                                          new HashSet<>(getAcceptors())));
        
        getEventBus().publish(
            ActorID.CONSENSUS_MANAGER,
            new SendMessageEvent(
                getAcceptors(), new SolicitConsensusMessage(
                    getNodeID(), getConsensusUnitID(), proposal)));
    }

}
