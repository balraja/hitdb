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

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.hit.consensus.raft.log.WAL;
import org.hit.consensus.raft.log.WALPropertyConfig;
import org.hit.event.SendMessageEvent;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.PoolUtils;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;
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
    @PoolConfiguration(size=10000, initialSize=100)
    public static class ProposalTracker implements Poolable
    {
        private final Set<NodeID> myLogAcceptors = new HashSet<>();
        
        private WAL myWriteAheadLog;
        
        private EventBus myEventBus;
        
        private Proposal myProposal;
        
        private NodeID myNodeID;
        
        private UnitID myConsensusUnitID;
        
        private State myState;
        
        private long myTermID;
        
        private long mySequenceNO;
        
        private long myLCTermNo;
        
        private long myLCSeqNo;
        
        /**
         * The factory method for instantiating the class
         */
        public static ProposalTracker create(
             EventBus    eventBus,
             WAL         wal,
             Proposal    proposal,
             NodeID      sender,
             Set<NodeID> acceptors,
             UnitID      unitID,
             long        termID,
             long        sequenceNO,
             long        lcTermID,
             long        lcSeqNo,
             State       state)
        {
            ProposalTracker tracker = 
                PooledObjects.getInstance(ProposalTracker.class);
            tracker.myNodeID          = sender;
            tracker.myConsensusUnitID = unitID;
            tracker.myEventBus        = eventBus;
            tracker.myWriteAheadLog   = wal;
            tracker.myProposal        = proposal;
            tracker.myLogAcceptors.addAll(acceptors);
            tracker.myTermID          = termID;
            tracker.mySequenceNO      = sequenceNO;
            tracker.myLCTermNo        = lcTermID;
            tracker.myLCSeqNo         = lcSeqNo;
            tracker.myState           = state;
            return tracker;
        }
        
        /**
         * Starts the commit process.
         */
        public void start()
        {
            myWriteAheadLog.addProposal(myTermID, mySequenceNO, myProposal);
            myEventBus.publish(
                ActorID.CONSENSUS_MANAGER,
                SendMessageEvent.create(                   
                    myLogAcceptors,
                    RaftReplicationMessage.create(
                        myNodeID,
                        myConsensusUnitID,
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
                myState.setCommitted(myTermID, mySequenceNO);
                myState.deleteTracker(myTermID, mySequenceNO);
                PoolUtils.free(myProposal);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void free()
        {
            myLogAcceptors.clear();
            myWriteAheadLog = null;
            myState = null;
            myNodeID = null;
            myConsensusUnitID = null;
            myEventBus = null;
            mySequenceNO = myLCSeqNo = myLCTermNo = myTermID = Long.MIN_VALUE;
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

        public Collection<ProposalTracker> 
            getTracker(long termID, long sequenceNo)
        {
            TLongObjectMap<ProposalTracker> seqMap = myProposalLog.get(termID);
            if (seqMap != null) {
                TLongObjectIterator<ProposalTracker> itr = seqMap.iterator();
                List<ProposalTracker> proposalTrackers = new ArrayList<>();
                while (itr.hasNext()) {
                    if (itr.key() <= sequenceNo) {
                        proposalTrackers.add(itr.value());
                    }
                }
                return proposalTrackers;
            }
            else{
                return Collections.<ProposalTracker>emptyList();
            }
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
                Collection<ProposalTracker> trackers = 
                    myProtocolState.getTracker(
                        response.getAcceptedTermID(),
                        response.getAcceptedSeqNo());
                
                if (!trackers.isEmpty()) {
                    for (ProposalTracker tracker : trackers) {
                        tracker.receivedAcceptance(response.getSenderId());
                    }
                }
            }
            else {
                // It can fail because:
                // a. Term is not known to be elected by the remote server
                // XXX We have to fix this.
                // b. Last seen sequence number by that server doesn't match
                //    the current sequence number.
                if (response.getAcceptedSeqNo() 
                        < myProtocolState.getSeqNumber())
                {
                    TLongObjectMap<Proposal> replayedProposals = 
                        myWAL.readProposalsFromLog(
                            response.getAcceptedTermID(),
                            response.getAcceptedSeqNo(),
                            myProtocolState.getSeqNumber());
                    
                    RaftReplayMessage replayMessage = 
                        new RaftReplayMessage(getNodeID(), 
                                              getConsensusUnitID(),
                                              response.getAcceptedTermID(), 
                                              replayedProposals);
                    
                    getEventBus().publish(ActorID.CONSENSUS_MANAGER,
                                          SendMessageEvent.create(
                                              response.getSenderId(), 
                                              replayMessage));
                }
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
           ProposalTracker.create(
               getEventBus(),
               myWAL, 
               proposal,
               getNodeID(), 
               getAcceptors(),
               getConsensusUnitID(), 
               myProtocolState.getTermNo(),
               myProtocolState.incAndGetSeqNum(),
               myProtocolState.getLastCommittedTermNo(),
               myProtocolState.getLastCommittedSeqNo(),
               myProtocolState);
        
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
