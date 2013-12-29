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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusAcceptor;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.ProposalNotificationResponse;
import org.hit.event.SendMessageEvent;
import org.hit.util.LogFactory;

/**
 * Extends <code>ConsensusAcceptor</code> to support driving consensus
 * in a two phase commit form of consensus.
 * 
 * @author Balraja Subbiah
 */
public class TwoPCAcceptor extends ConsensusAcceptor
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(TwoPCAcceptor.class);
            
    private final Map<Proposal, SolicitConsensusMessage> myProposalToConsensusMap;
    
    /**
     * CTOR
     */
    public TwoPCAcceptor(
        UnitID consensusUnitID,
        NodeID leader,
        EventBus eventBus,
        NodeID myID)
    {
        super(consensusUnitID, leader, eventBus, myID);
        myProposalToConsensusMap = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(Message message)
    {
        if (message instanceof SolicitConsensusMessage) {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Received consensus request from " 
                         + message.getSenderId());
            }
            SolicitConsensusMessage scm = (SolicitConsensusMessage) message;
            myProposalToConsensusMap.put(scm.getProposal(), scm);
            getEventBus().publish(
                ActorID.CONSENSUS_MANAGER,
                new ProposalNotificationEvent(scm.getProposal()));
        }
        else if (message instanceof CommitRequest) {
            CommitRequest commitRequest = (CommitRequest) message;
            if (LOG.isLoggable(Level.FINE)) {
                LOG.info("Received commit " + commitRequest.shouldCommit()
                         + " for " + commitRequest.getProposal()
                         + " from " + commitRequest.getSenderId());
            }
            getEventBus().publish(
                ActorID.CONSENSUS_MANAGER,
                new ProposalNotificationEvent(
                   commitRequest.getProposal(), 
                   true, 
                   commitRequest.shouldCommit()));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public
        void handleResponse(ProposalNotificationResponse response)
    {
        SolicitConsensusMessage scm = 
            myProposalToConsensusMap.get(
                response.getProposalNotification().getProposal());
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.info("Publishing " + response.canAccept() + " for proposal "
                    + scm.getProposal() + " from " + scm.getSenderId());
        }
        
        getEventBus().publish(
            ActorID.CONSENSUS_MANAGER,
            new SendMessageEvent(
                Collections.singleton(scm.getSenderId()),
                new ConsensusAcceptMessage(getNodeID(),
                                           getConsensusUnitID(),
                                           scm.getProposal(),
                                           response.canAccept())));
    }

}
