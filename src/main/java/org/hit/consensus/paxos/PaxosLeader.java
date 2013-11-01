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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.event.ConsensusResponseEvent;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.SendMessageEvent;
import org.hit.util.LogFactory;
import org.hit.util.Pair;

/**
 * Implementation of {@link ConsensusLeader} using paxos protocol.
 * 
 * <ol>
 * <li> A proposer selects a proposal number N and sends a
 *  {@link PaxosSolicitConsensusMessage} request to all the acceptors </li>
 *  <li> On receiving {@link PaxosConsensusAcceptMessage} from majority of the
 *  acceptors, if they have agreed to accept this message, then send
 *  {@link CommitRequestMessage} to the accepted acceptors. Else abort the
 *  consensus.
 *  </li>
 * </ol>
 * 
 * @author Balraja Subbiah
 */
public class PaxosLeader extends ConsensusLeader
{
    /**
     * Bookkeeping information about the state of {@link Proposal}s submitted
     * to the leader for consensus.
     */
    private static class ProposalRecord
    {
        private final PaxosProposal myProposal;
        
        private State myState;
        
        private final Set<NodeID> myAcceptors;
        
        private final Set<NodeID> myAcceptedAcceptors;
        
        private final Map<NodeID, Pair<Long, Proposal>> myAcceptedResult;

        /**
         * CTOR
         */
        public ProposalRecord(PaxosProposal    proposal,
                              Set<NodeID>      acceptors)
        {
            myProposal = proposal;
            myState = State.NOT_STARTED;
            myAcceptors = acceptors;
            myAcceptedAcceptors = new HashSet<>();
            myAcceptedResult = new HashMap<>();
        }

        /**
         * Adds the given acceptors to the list who committed the seq number
         * of the proposal.
         */
        public void addAcceptedAcceptor(NodeID   nodeID,
                                        long     lastAcceptedSeqID,
                                        Proposal lastAcceptedProposal)
        {
            myAcceptedAcceptors.add(nodeID);
            myAcceptedResult.put(nodeID, new Pair<>(lastAcceptedSeqID,
                                                    lastAcceptedProposal));
        }
        
        /**
         * Adds the given acceptors to the list who accepted the seq number
         * of the proposal.
         */
        public void addCommittedAcceptor(NodeID nodeID)
        {
            myAcceptedAcceptors.remove(nodeID);
        }

        /**
         * Returns true if the majority has accepted the slot and the proposal
         * contains the values of all the proposals accepted previously by the
         * acceptors.
         */
        public boolean canInitiateCommit()
        {
            return myAcceptedAcceptors.size() > (myAcceptors.size() / 2)
                   && canPublishProposal();
        }

        private boolean canPublishProposal()
        {
            for (Pair<Long, Proposal> returnedProposal :
                    myAcceptedResult.values())
            {
                if (!myProposal.contains(returnedProposal.getSecond())) {
                    return false;
                }
            }
            return true;
        }
        
        /** Changes the {@link State} of the proposal wrt leader */
        public void changeState(State state)
        {
            myState = state;
        }
        
        /**
         * Returns the value of acceptedAcceptors who has accepted this
         * proposal.
         */
        public Set<NodeID> getAcceptedAcceptors()
        {
            return myAcceptedAcceptors;
        }
        
        /**
         * Returns the value of all acceptors to which the given proposal
         * is sent.
         */
        public Set<NodeID> getAcceptors()
        {
            return myAcceptors;
        }

        /**
         * Returns the value of proposal
         */
        public PaxosProposal getProposal()
        {
            return myProposal;
        }
        
        /**
         * Returns the value of state
         */
        public State getState()
        {
            return myState;
        }
        
        /** Returns true if all the acceptors have committed the changes */
        public boolean isCommitted()
        {
            return myAcceptedAcceptors.isEmpty();
        }
    }
    
    /**
     * Defines the contract for various states of the proposal submitted
     * for a consensus.
     */
    private static enum State
    {
        NOT_STARTED,
        ACCEPT_REQUEST_SENT,
        REQUEST_CONFIRMED,
        REQUEST_DENIED,
        COMMIT_REQUEST_SENT,
        COMMITTED,
        REJECTED
    }
    
    private static final Logger LOG =
         LogFactory.getInstance().getLogger(PaxosLeader.class);
    
    private final Map<Long, ProposalRecord> myReceivedProposals;

    /**
     * CTOR
     */
    public PaxosLeader(UnitID      consensusUnitID,
                       Set<NodeID> acceptors,
                       EventBus    eventBus,
                       NodeID      myID)
    {
        super(consensusUnitID, acceptors, eventBus, myID);
        myReceivedProposals = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getConsensus(Proposal proposal)
    {
        assert proposal instanceof PaxosProposal;
        PaxosProposal paxosProposal = (PaxosProposal) proposal;
        ProposalRecord record =
            new ProposalRecord(paxosProposal, getAcceptors());
        
        myReceivedProposals.put(paxosProposal.getSequenceNumber(), record);
        sendMessage(
            getAcceptors(),
            new PaxosSolicitConsensusMessage(
                getNodeID(),
                getConsensusUnitID(),
                paxosProposal,
                paxosProposal.getSequenceNumber()));
        
        record.changeState(State.ACCEPT_REQUEST_SENT);
    }
    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(Message message)
    {
        if (message instanceof PaxosConsensusAcceptMessage) {
            PaxosConsensusAcceptMessage pcam =
                (PaxosConsensusAcceptMessage) message;
            
            ProposalRecord record =
                myReceivedProposals.get(pcam.getAcceptedID());
            
            if (record.getState() != State.ACCEPT_REQUEST_SENT) {
                return;
            }
            
            if (pcam.isAccepted()) {
                record.addAcceptedAcceptor(pcam.getSenderId(),
                                           pcam.getPreviousAcceptedID(),
                                           pcam.getPreviouslyAcceptedProposal());
            }
            
            if (record.canInitiateCommit()) {
                getEventBus().publish(
                    new ProposalNotificationEvent(
                        record.getProposal(), true, true)
                );
                
                sendMessage(
                    getAcceptors(),
                    new PaxosCommitRequest(
                        getNodeID(),
                        getConsensusUnitID(),
                        record.getProposal(),
                        record.getProposal().getSequenceNumber()));
                
                record.changeState(State.COMMIT_REQUEST_SENT);
            }
        }
        else if (message instanceof PaxosCommitResponse) {
            PaxosCommitResponse pcm = (PaxosCommitResponse) message;
            
            ProposalRecord record =
                myReceivedProposals.get(pcm.getSequenceID());
            
            if (record.getState() != State.COMMIT_REQUEST_SENT) {
                return;
            }
                        
            if (pcm.isAccepted()) {
                record.addCommittedAcceptor(pcm.getSenderId());
            }
            
            if (record.isCommitted()) {
                record.changeState(State.COMMITTED);
                getEventBus().publish(new ConsensusResponseEvent(
                    record.getProposal(), true));
            }
        }
    }
    
    private void sendMessage(Collection<NodeID> acceptors, Message message)
    {
        getEventBus().publish(
            new SendMessageEvent(acceptors, message));
    }
}
