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

package org.hit.consensus.paxos;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusAcceptor;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.ProposalNotificationResponse;
import org.hit.event.SendMessageEvent;
import org.hit.util.LogFactory;

/**
 * Defines the implementation of {@link ConsensusAcceptor} that works in
 * coordination with the {@link ConsensusLeader}.
 * 
 * @author Balraja Subbiah
 */
public class PaxosAcceptor extends ConsensusAcceptor
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(PaxosAcceptor.class);
                                                       
    private long myLastSeqAccepted;
    
    private final Proposal myLastProposalCommitted;
    
    private final Map<Proposal, Long> myProposalToSeqNumMap;

    /**
     * CTOR
     */
    public PaxosAcceptor(UnitID consensusUnitID,
                         NodeID leader,
                         EventBus eventBus,
                         NodeID myID)
    {
        super(consensusUnitID, leader, eventBus, myID);
        myLastSeqAccepted = -1L;
        myLastProposalCommitted = null;
        myProposalToSeqNumMap = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(Message message)
    {
        if (message instanceof PaxosSolicitConsensusMessage) {
            PaxosSolicitConsensusMessage pscm =
                (PaxosSolicitConsensusMessage) message;
            
            if (pscm.getRequestID() > myLastSeqAccepted) {
                myLastSeqAccepted = pscm.getRequestID();
                myProposalToSeqNumMap.put(pscm.getProposal(), 
                                          pscm.getRequestID());
                getEventBus().publish(
                    new ProposalNotificationEvent(pscm.getProposal())
                );
            }
            else {
                getEventBus().publish(new SendMessageEvent(
                    Collections.singletonList(pscm.getSenderId()),
                    new PaxosConsensusAcceptMessage(getNodeID(),
                                                    getConsensusUnitID(),
                                                    pscm.getProposal(),
                                                    pscm.getRequestID(),
                                                    false,
                                                    myLastSeqAccepted,
                                                    myLastProposalCommitted))); 
            }
        }
        else if (message instanceof PaxosCommitRequest) {
            PaxosCommitRequest pcr =
                (PaxosCommitRequest) message;
            
            if (pcr.getSequenceID() <= myLastSeqAccepted) {
                myProposalToSeqNumMap.put(pcr.getProposal(), 
                                          pcr.getSequenceID());
                getEventBus().publish(
                    new ProposalNotificationEvent(pcr.getProposal(), true)
                );
            }
            else {
                getEventBus().publish(new PaxosCommitResponse(
                    getNodeID(),
                    getConsensusUnitID(),
                    pcr.getProposal(),
                    false,
                    pcr.getSequenceID()));
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleResponse(ProposalNotificationResponse response)
    {
        long seqNumber = 
            myProposalToSeqNumMap.get(response.getProposalNotification()
                                              .getProposal());
        
        if (!response.getProposalNotification().isCommitNotification()) {
            
            getEventBus().publish(new SendMessageEvent(
                Collections.singletonList(getLeader()),
                new PaxosConsensusAcceptMessage(getNodeID(),
                                                getConsensusUnitID(),
                                                response.getProposalNotification()
                                                        .getProposal(),
                                                seqNumber,
                                                response.canAccept(),
                                                myLastSeqAccepted,
                                                myLastProposalCommitted)));
        }
        else {
            getEventBus().publish(new PaxosCommitResponse(
                getNodeID(),
                getConsensusUnitID(),
                response.getProposalNotification().getProposal(),
                response.canAccept(),
                seqNumber));
        }
        
    }
}
