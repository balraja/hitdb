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

import java.util.Set;

import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;


/**
 * Defines the contract for an abstract consensus leader protocol. In a
 * consensus protocol leader proposes values with the peers for achieving
 * consensus for the values..
 * 
 * @author Balraja Subbiah
 */
public abstract class ConsensusLeader extends AbstractConsensusProtocol
{
    private final Set<NodeID> myAcceptors;
    
    /**
     * CTOR
     */
    public ConsensusLeader(UnitID      consensusUnitID,
                           Set<NodeID> acceptors,
                           EventBus    eventBus,
                           NodeID      myID)
    {
        super(consensusUnitID, true, eventBus, myID);
        myAcceptors       = acceptors;
    }

    /**
     * Returns the value of acceptors
     */
    public Set<NodeID> getAcceptors()
    {
        return myAcceptors;
    }
    
    /**
     * Use the protocol to achieve consensus for the given proposal.
     */
    public abstract void getConsensus(Proposal proposal);
}
