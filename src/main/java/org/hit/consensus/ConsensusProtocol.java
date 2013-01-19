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

import org.hit.communicator.Message;

/**
 * Defines the contract for a protocol to achieve consensus among it's peers.
 * 
 * @author Balraja Subbiah
 */
public interface ConsensusProtocol
{
    
    /**
     * Returns the identifier for a unit that's guarded by a consensus
     * protocol.
     */
    public UnitID getConsensusUnitID();
    
    /**
     * Handles the given {@link Message}s that will help us in achieving the
     * consensus.
     */
    public void handle(Message message);
    
    /**
     * Returns true if this protocol acts as a leader or false if it
     * acts as an acceptor.
     */
    public boolean isProposer();
}
