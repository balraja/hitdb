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

import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusAcceptor;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.ConsensusProtocolProvider;
import org.hit.event.CreateConsensusAcceptorEvent;
import org.hit.event.CreateConsensusLeaderEvent;
import org.hit.event.CreateRaftAcceptorEvent;
import org.hit.event.CreateRaftLeaderEvent;

/**
 * Extends {@link ConsensusProtocolProvider} to support creating 
 * {@link RaftLeader}s and {@link RaftAcceptor}s.
 * 
 * @author Balraja Subbiah
 */
public class RaftProvider implements ConsensusProtocolProvider
{

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsensusAcceptor makeAcceptor(
        CreateConsensusAcceptorEvent cae, EventBus eventBus, NodeID ourNodeID)
    {
        CreateRaftAcceptorEvent crae = (CreateRaftAcceptorEvent) cae;
        return new RaftAcceptor(
            crae.getUnitID(),
            crae.getLeader(),
            eventBus,
            ourNodeID,
            crae.getTermID());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsensusLeader makeLeader(
        CreateConsensusLeaderEvent cle, EventBus eventBus, NodeID ourNodeID)
    {
        CreateRaftLeaderEvent crle = (CreateRaftLeaderEvent) cle;
        return new RaftLeader(
            crle.getUnitID(),
            crle.getAcceptors(),
            eventBus,
            ourNodeID,
            crle.getTermID());
    }

}
