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

package org.hit.consensus;

import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;

/**
 * Defines the contract for an acceptor in a consensus protocol. The node
 * running an acceptor holds the state replica and participates in the
 * consensus to ensure that same set of state transition is applied on each
 * replica.
 * 
 * @author Balraja Subbiah
 */
public abstract class ConsensusAcceptor extends AbstractConsensusProtocol
{
    private final NodeID      myLeader;
    
    /**
     * CTOR
     */
    public ConsensusAcceptor(UnitID   consensusUnitID,
                             NodeID   leader,
                             EventBus eventBus,
                             NodeID   myID)
    {
        super(consensusUnitID, false, eventBus, myID);
        myLeader = leader;
    }

    /**
     * Returns the value of leader
     */
    public NodeID getLeader()
    {
        return myLeader;
    }
}
