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
package org.hit.consensus.raft;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.concurrent.pool.PooledObjects;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.consensus.raft.log.WAL;
import org.hit.consensus.raft.log.WALPropertyConfig;
import org.hit.event.SendMessageEvent;
import org.hit.util.LogFactory;

/**
 * Extends {@link ConsensusLeader} to support raft specific version.
 * 
 * @author Balraja Subbiah
 */
public class RaftLeader extends ConsensusLeader implements RaftProtocol
{
    /** LOGGER */
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(RaftLeader.class);
                
    /**
     * A simple class to keep track of progress of a {@link Proposal}
     * being replicated across the followers.
     */
    private class ProposalTracker
    {
        private final Set<NodeID> myLogAcceptors;
        
        private final Proposal myProposal;
        
        private final long myTermID;
        
        private final long mySequenceNO;
        
        private final long myLCTermNo;
        
        private final long myLCSeqNo;
        
        /**
         * CTOR
         */
        public ProposalTracker(
             Proposal proposal, 
             long     termID,
             long     sequenceNO,
             long     lcTermID,
             long     lcSeqNo)
        {
            myProposal        = proposal;
            myLogAcceptors    = new HashSet<>(getAcceptors());
            myTermID          = termID;
            mySequenceNO      = sequenceNO;
            myLCTermNo        = lcTermID;
            myLCSeqNo         = lcSeqNo;
        }
        
        /**
         * Starts the commit process.
         */
        public void start()
        {
            myWAL.addProposal(myTermID, mySequenceNO, myProposal);
            getEventBus().publish(
                ActorID.CONSENSUS_MANAGER,
                PooledObjects.getInstance(SendMessageEvent.class).initialize(                   
                    myLogAcceptors,
                    new RaftReplicationMessage(getNodeID(),
                                               getConsensusUnitID(),
                                               myProposal, 
                                               myTermID, 
                                               mySequenceNO,
                                               myLCTermNo,
                                               myLCSeqNo)));
        }
        
        public void receivedAcceptance(NodeID acceptedNodeID)
        {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Received acceptance for " 
                         + myTermID 
                         + " : " 
                         + mySequenceNO
                         + " from " + acceptedNodeID);
            }
            myLogAcceptors.remove(acceptedNodeID);
            if (myLogAcceptors.isEmpty()) {
                myProtocolState.setCommitted(myTermID, mySequenceNO);
                myProtocolState.deleteTracker(myTermID, mySequenceNO);
            }
        }
    }
    
    /**
     * This class tracks the progress of all {@link Proposal}s submitted
     * using their {@link ProposalTracker}s.
     */
    private static class State
    {
        private final TLongObjectMap<TLongObjectMap<ProposalTracker>> myProposalLog;
        
        private long myTermNo;
        
        private long mySequenceNo;
        
        private long myLastCommittedTermNo;
        
        private long myLastCommittedSeqNo;
        
        /**
         * CTOR
         */
        public State(long termNo)
        {
            myProposalLog = new TLongObjectHashMap<>();
            myTermNo      = termNo;
            mySequenceNo  = -1L;
            myLastCommittedSeqNo = -1L;
            myLastCommittedTermNo = -1L;
        }

        /**
         * Returns the value of termNo
         */
        public long getTermNo()
        {
            return myTermNo;
        }

        /**
         * Sets the value of termNo
         */
        public void setTermNo(long termNo)
        {
            if (termNo != myTermNo) {
                myTermNo = termNo;
                mySequenceNo = 1L;
            }
        }
        
        public long incAndGetSeqNum()
        {
            return ++mySequenceNo;
        }
        
        public long getSeqNumber()
        {
            return mySequenceNo;
        }
        
        /**
         * Returns the value of lastCommittedTermNo
         */
        public long getLastCommittedTermNo()
        {
            return myLastCommittedTermNo;
        }

        /**
         * Returns the value of lastCommittedSeqNo
         */
        public long getLastCommittedSeqNo()
        {
            return myLastCommittedSeqNo;
        }

        public ProposalTracker getTracker(long termID, long sequenceNo)
        {
            TLongObjectMap<ProposalTracker> seqMap = myProposalLog.get(termID);
            return seqMap != null ? seqMap.get(sequenceNo) : null;
        }
        
        public void setTracker(
            long termID, long sequenceNo, ProposalTracker supervisor)
        {
            TLongObjectMap<ProposalTracker> seqMap = myProposalLog.get(termID);
            if (seqMap == null) {
                seqMap = new TLongObjectHashMap<>();
                myProposalLog.put(termID, seqMap);
            }
            seqMap.put(sequenceNo, supervisor);
        }
        
        public void deleteTracker(long termID, long sequenceNo)
        {
            TLongObjectMap<ProposalTracker> seqMap =
                myProposalLog.get(termID);
            if (seqMap != null) {
                seqMap.remove(sequenceNo);
            }
        }
        
        public void setCommitted(long termNo, long sequenceNo)
        {
            myLastCommittedTermNo = termNo;
            myLastCommittedSeqNo  = sequenceNo;
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("The last committed proposal is  " 
                         + myLastCommittedTermNo
                         + " : " 
                         + myLastCommittedSeqNo);
            }
        }
    }
    
    private final State myProtocolState;
    
    private final WAL myWAL;
    
    /**
     * CTOR
     */
    public RaftLeader(
        UnitID consensusUnitID,
        Set<NodeID> acceptors,
        EventBus eventBus,
        NodeID myID,
        long termID)
    {
        super(consensusUnitID, acceptors, eventBus, myID);
        myProtocolState = new State(termID);
        myWAL = new WAL(new WALPropertyConfig(consensusUnitID.toString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(Message message)
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("The raft leader corresponding to " + getConsensusUnitID()
                    + " has received " + message.getClass()
                    + " from " + message.getSenderId());
        } 
        
        if (message instanceof RaftReplicationResponse) {
            RaftReplicationResponse response = 
                (RaftReplicationResponse) message;
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Replication response "
                          + response.getAcceptedTermID() 
                          + " : "
                          + response.getAcceptedSeqNo()
                          + " is accepted " 
                          + response.isAccepted());
            } 
            
            if (response.isAccepted()) {
                ProposalTracker supervisor = 
                    myProtocolState.getTracker(
                        response.getAcceptedTermID(),
                        response.getAcceptedSeqNo());
                if (supervisor != null) {
                    supervisor.receivedAcceptance(response.getSenderId());
                }
            }
            else {
                // It can fail because:
                // a. Term is not known to be elected by the remote server
                // b. Last seen sequence number by that server doesn't match
                //    the current sequence number.
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getConsensus(Proposal proposal)
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Received " + proposal + " for replication");
            LOG.fine("The current term-number:seq-no is  " 
                     + myProtocolState.getTermNo()
                     + " : "
                     + myProtocolState.getSeqNumber());
        }
        ProposalTracker trace = 
           new ProposalTracker(
               proposal,
               myProtocolState.getTermNo(),
               myProtocolState.incAndGetSeqNum(),
               myProtocolState.getLastCommittedTermNo(),
               myProtocolState.getLastCommittedSeqNo());
        
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("The term-number:seq-no assigned is  " 
                     + myProtocolState.getTermNo()
                     + " : "
                     + myProtocolState.getSeqNumber()
                     + " and replicated across the nodes " 
                     + getAcceptors());
        }
        
        myProtocolState.setTracker(
            trace.myTermID, trace.mySequenceNO, trace);
        
        trace.start();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleTermChange(long newTermID)
    {
        myProtocolState.setTermNo(newTermID);
    }
}
