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
 * Defines an abstract implementation of {@link ConsensusProtocol} that
 * acts as a place holder for common properties.
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractConsensusProtocol implements ConsensusProtocol
{
    private final UnitID      myConsensusUnitID;
    
    private final boolean     myIsProposer;
    
    private final EventBus    myEventBus;
    
    private final NodeID      myNodeID;
    
    /**
     * CTOR
     */
    public AbstractConsensusProtocol(UnitID   consensusUnitID,
                                     boolean  proposer,
                                     EventBus eventBus,
                                     NodeID   nodeID)
    {
        myConsensusUnitID = consensusUnitID;
        myIsProposer      = proposer;
        myNodeID          = nodeID;
        myEventBus        = eventBus;
    }
    
    /**
     * Returns the identifier for a unit that's guarded by a consensus
     * protocol.
     */
    @Override
    public UnitID getConsensusUnitID()
    {
        return myConsensusUnitID;
    }

    /**
     * Returns the value of eventBus
     */
    protected EventBus getEventBus()
    {
        return myEventBus;
    }

    /**
     * Returns the value of nodeID
     */
    protected NodeID getNodeID()
    {
        return myNodeID;
    }

    /**
     * Returns the value of isProposer
     */
    @Override
    public boolean isProposer()
    {
        return myIsProposer;
    }
}
