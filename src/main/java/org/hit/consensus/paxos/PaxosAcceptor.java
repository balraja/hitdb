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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusAcceptor;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
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
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(Message message)
    {
        try {
            if (message instanceof PaxosSolicitConsensusMessage) {
                PaxosSolicitConsensusMessage pscm =
                    (PaxosSolicitConsensusMessage) message;
                
                if (pscm.getRequestID() > myLastSeqAccepted) {
                    myLastSeqAccepted = pscm.getRequestID();
                }
                getEventBus().publish(new SendMessageEvent(
                       Collections.singletonList(pscm.getNodeId()),
                       new PaxosConsensusAcceptMessage(getNodeID(),
                                                       getConsensusUnitID(),
                                                       pscm.getRequestID(),
                                                       true,
                                                       myLastSeqAccepted,
                                                       myLastProposalCommitted)));
            }
            else if (message instanceof PaxosCommitRequest) {
                PaxosCommitRequest pcr =
                    (PaxosCommitRequest) message;
                
                boolean isAccepted = false;
                if (pcr.getSequenceID() <= myLastSeqAccepted) {
                    isAccepted = true;
                }
                
                getEventBus().publish(new PaxosCommitResponse(
                    getNodeID(),
                    getConsensusUnitID(),
                    isAccepted,
                    pcr.getSequenceID()));
            }
        }
        catch (EventBusException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
    
}
