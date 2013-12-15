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

import java.util.Arrays;
import java.util.Collections;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.procedure.TLongObjectProcedure;
import gnu.trove.procedure.TLongProcedure;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusAcceptor;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.consensus.raft.log.WAL;
import org.hit.consensus.raft.log.WALPropertyConfig;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.ProposalNotificationResponse;
import org.hit.event.SendMessageEvent;

/**
 * @author Balraja Subbiah
 */
public class RaftAcceptor extends ConsensusAcceptor 
    implements RaftProtocol
{
    private long myTermID;
    
    private final TLongObjectMap<TLongObjectMap<Proposal>> myProposalLog;
    
    private final WAL myWAL;
    
    /**
     * CTOR
     */
    public RaftAcceptor(
        UnitID consensusUnitID,
        NodeID leader,
        EventBus eventBus,
        NodeID myID,
        long termID)
    {
        super(consensusUnitID, leader, eventBus, myID);
        myTermID = termID;
        myProposalLog = new TLongObjectHashMap<>();
        myWAL = new WAL(new WALPropertyConfig(consensusUnitID.toString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handle(Message message)
    {
        if (message instanceof RaftReplicationMessage) {
            final RaftReplicationMessage replicationMessage = 
                (RaftReplicationMessage) message;
            
            if (replicationMessage.getTermID() > myTermID) {
                // Reject if it comes from a new term that's not known 
                // to be elected.
                getEventBus().publish(
                    ActorID.CONSENSUS_MANAGER,
                    new SendMessageEvent(
                        Collections.singleton(replicationMessage.getSenderId()),
                        new RaftReplicationResponse(
                            replicationMessage.getSenderId(),
                            replicationMessage.getUnitID())));
            }
            
            TLongObjectMap<Proposal> seqMap =
                myProposalLog.get(
                    replicationMessage.getTermID());
            
            if (seqMap == null) {
                seqMap = new TLongObjectHashMap<>();
                myProposalLog.put(
                    replicationMessage.getTermID(), seqMap);
            }
            else {
                long[] sequenceNumbers = seqMap.keys();
                Arrays.sort(sequenceNumbers);
                // Reject if we receive a request if it's sequence 
                // number is not equal to 1 + the max sequence
                // number known for that term.
                if ((replicationMessage.getSequenceNumber() - 1) == 
                        sequenceNumbers[sequenceNumbers.length - 1])
                {
                    getEventBus().publish(
                        ActorID.CONSENSUS_MANAGER,
                        new SendMessageEvent(
                            Collections.singleton(replicationMessage.getSenderId()),
                            new RaftReplicationResponse(
                                replicationMessage.getSenderId(),
                                replicationMessage.getUnitID())));
                    return;
                }
            }
            seqMap.put(replicationMessage.getSequenceNumber(), 
                       replicationMessage.getProposal());
            myWAL.addProposal(
                replicationMessage.getTermID(),
                replicationMessage.getSequenceNumber(),
                replicationMessage.getProposal());
            
            getEventBus().publish(
                ActorID.CONSENSUS_MANAGER,
                new SendMessageEvent(
                    Collections.singleton(replicationMessage.getSenderId()),
                    new RaftReplicationResponse(
                        replicationMessage.getSenderId(),
                        replicationMessage.getUnitID(),
                        true,
                        replicationMessage.getTermID(),
                        replicationMessage.getSequenceNumber())));
            
            final TLongObjectMap<Proposal> termLog = 
                myProposalLog.get(
                    replicationMessage.getLastCommittedTermID());
            
            final TLongSet removedKeys = new TLongHashSet();
            termLog.forEachEntry(new TLongObjectProcedure<Proposal>() {

                @Override
                public boolean execute(long key, Proposal value)
                {
                    if (key <= replicationMessage.getLastCommittedSeqNo())
                    {
                        getEventBus().publish(
                            ActorID.CONSENSUS_MANAGER,
                            new ProposalNotificationEvent(
                                value, true, true));
                        removedKeys.add(key);
                    }
                    return true;
                }
            });
            
            removedKeys.forEach(new TLongProcedure() {
                
                @Override
                public boolean execute(long key)
                {
                    termLog.remove(key);
                    return true;
                }
            });
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleTermChange(long newTermID)
    {
        myTermID = newTermID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleResponse(ProposalNotificationResponse response)
    {
    }
}
