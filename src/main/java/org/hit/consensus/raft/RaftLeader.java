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

import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.consensus.raft.log.WAL;
import org.hit.consensus.raft.log.WALPropertyConfig;
import org.hit.event.ConsensusResponseEvent;
import org.hit.event.SendMessageEvent;

/**
 * Extends {@link ConsensusLeader} to support raft specific version.
 * 
 * @author Balraja Subbiah
 */
public class RaftLeader extends ConsensusLeader 
    implements RaftProtocol
{
    private class Trace
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
        public Trace(Proposal proposal, 
                          long     termID,
                          long     sequenceNO,
                          long     lcTermID,
                          long     lcSeqNo)
        {
            myProposal        = proposal;
            myLogAcceptors    = new HashSet<>(getAcceptors());
            myTermID          = termID;
            mySequenceNO      = sequenceNO;
            myLCTermNo        = termID;
            myLCSeqNo         = lcSeqNo;
        }
        
        /**
         * Starts the commit process.
         */
        public void start()
        {
            myWAL.addProposal(myTermID, mySequenceNO, myProposal);
            getEventBus().publish(
                new SendMessageEvent(
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
            myLogAcceptors.remove(acceptedNodeID);
            if (myLogAcceptors.isEmpty()) {
                myProtocolState.setCommitted(myTermID, mySequenceNO);
                getEventBus().publish(new ConsensusResponseEvent(
                    myProposal, true));
                myProtocolState.deleteSupervisor(myTermID, mySequenceNO);
            }
        }
    }
    
    private static class State
    {
        private final TLongObjectMap<TLongObjectMap<Trace>> myProposalLog;
        
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
            mySequenceNo  = 1L;
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

        public Trace getSuperVisor(long termID, long sequenceNo)
        {
            TLongObjectMap<Trace> seqMap =
                myProposalLog.get(termID);
            return seqMap != null ? seqMap.get(sequenceNo) : null;
        }
        
        public void setSupervisor(long termID, 
                                  long sequenceNo, 
                                  Trace supervisor)
        {
            TLongObjectMap<Trace> seqMap =
                myProposalLog.get(termID);
            if (seqMap == null) {
                seqMap = new TLongObjectHashMap<>();
                myProposalLog.put(termID, seqMap);
            }
            seqMap.put(sequenceNo, supervisor);
        }
        
        public void deleteSupervisor(long termID, long sequenceNo)
        {
            TLongObjectMap<Trace> seqMap =
                myProposalLog.get(termID);
            if (seqMap != null) {
                seqMap.remove(sequenceNo);
            }
        }
        
        public void setCommitted(long termNo, long sequenceNo)
        {
            myLastCommittedTermNo = termNo;
            myLastCommittedSeqNo  = sequenceNo;
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
        if (message instanceof RaftReplicationResponse) {
            RaftReplicationResponse response = 
                (RaftReplicationResponse) message;
            Trace supervisor = 
                myProtocolState.getSuperVisor(
                    response.getAcceptedTermID(),
                    response.getAcceptedSeqNo());
            if (supervisor != null) {
                supervisor.receivedAcceptance(response.getSenderId());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getConsensus(Proposal proposal)
    {
        Trace trace = 
            new Trace(proposal,
                       myProtocolState.getTermNo(),
                       myProtocolState.incAndGetSeqNum(),
                       myProtocolState.getLastCommittedTermNo(),
                       myProtocolState.getLastCommittedSeqNo());
        
        myProtocolState.setSupervisor(
            trace.myTermID,
            trace.mySequenceNO,
            trace);
        
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
