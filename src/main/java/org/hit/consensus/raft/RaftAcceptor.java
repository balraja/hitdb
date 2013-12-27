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
import java.util.logging.Level;
import java.util.logging.Logger;

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
import org.hit.util.LogFactory;

/**
 * Extends {@link ConsensusAcceptor} to play it's role in the raft protocol
 * to receive and process the replication requests.
 * 
 * @author Balraja Subbiah
 */
public class RaftAcceptor extends ConsensusAcceptor 
    implements RaftProtocol
{
    /** LOGGER */
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(RaftAcceptor.class);
                
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
                LOG.severe("Received replication request : " 
                           + replicationMessage.getProposal()
                           + " for " 
                           + replicationMessage.getTermID()
                           + " : "
                           + replicationMessage.getSequenceNumber()
                           + " from " + replicationMessage.getSenderId()
                           + " and the term is not known to be accepted");
                
                getEventBus().publish(
                    ActorID.CONSENSUS_MANAGER,
                    new SendMessageEvent(
                        Collections.singleton(replicationMessage.getSenderId()),
                        new RaftReplicationResponse(
                            getNodeID(),
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
                if ((replicationMessage.getSequenceNumber() - 1) != 
                        sequenceNumbers[sequenceNumbers.length - 1])
                {
                    LOG.severe("Received replication request : " 
                            + replicationMessage.getProposal()
                            + " for " 
                            + replicationMessage.getTermID()
                            + " : "
                            + replicationMessage.getSequenceNumber()
                            + " from " + replicationMessage.getSenderId()
                            + " and the seqno doesn't match 1 + "
                            + sequenceNumbers[sequenceNumbers.length - 1]);
                    
                    getEventBus().publish(
                        ActorID.CONSENSUS_MANAGER,
                        new SendMessageEvent(
                            Collections.singleton(replicationMessage.getSenderId()),
                            new RaftReplicationResponse(
                                getNodeID(),
                                replicationMessage.getUnitID())));
                    return;
                }
            }
            seqMap.put(replicationMessage.getSequenceNumber(), 
                       replicationMessage.getProposal());
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("The replication proposal : " 
                        + replicationMessage.getProposal()
                        + " for " 
                        + replicationMessage.getTermID()
                        + " : "
                        + replicationMessage.getSequenceNumber()
                        + " from " + replicationMessage.getSenderId()
                        + " is added to the replication log");
            }
            
            myWAL.addProposal(
                replicationMessage.getTermID(),
                replicationMessage.getSequenceNumber(),
                replicationMessage.getProposal());
            
            getEventBus().publish(
                ActorID.CONSENSUS_MANAGER,
                new SendMessageEvent(
                    Collections.singleton(replicationMessage.getSenderId()),
                    new RaftReplicationResponse(
                        getNodeID(),
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
                        
                        if (LOG.isLoggable(Level.FINE)) {
                            LOG.fine("The replication proposal : " 
                                    + value
                                    + " at " 
                                    + replicationMessage.getLastCommittedTermID()
                                    + " : "
                                    + key
                                    + " is advertised to the clients ");
                        }
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
