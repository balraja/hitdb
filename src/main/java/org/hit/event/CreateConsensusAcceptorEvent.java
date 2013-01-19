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

package org.hit.event;

import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusAcceptor;
import org.hit.consensus.ConsensusProtocol;
import org.hit.consensus.UnitID;

/**
 * Defines the contract for <code>Event</code> that's responsible for
 * creating an {@link ConsensusAcceptor} that participates in a
 * {@link ConsensusProtocol}.
 * 
 * @author Balraja Subbiah
 */
public class CreateConsensusAcceptorEvent implements Event
{
    private final UnitID myUnitID;
    
    private final NodeID myLeader;

    /**
     * CTOR
     */
    public CreateConsensusAcceptorEvent(UnitID unitID, NodeID leader)
    {
        super();
        myUnitID = unitID;
        myLeader = leader;
    }

    /**
     * Returns the value of leader
     */
    public NodeID getLeader()
    {
        return myLeader;
    }

    /**
     * Returns the value of unitID
     */
    public UnitID getUnitID()
    {
        return myUnitID;
    }
}
