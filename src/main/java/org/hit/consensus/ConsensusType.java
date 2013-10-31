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
package org.hit.consensus;

import org.hit.consensus.paxos.PaxosProvider;
import org.hit.consensus.twopc.TwoPCProvider;

/**
 * Defines an enum that specifies various types of consensus available.
 * 
 * @author Balraja Subbiah
 */
public enum ConsensusType
{
    PAXOS {

        @Override
        public
            ConsensusProtocolProvider makeProvider()
        {
            return new PaxosProvider();
        }
        
    },
    TW0_PC {

        @Override
        public
            ConsensusProtocolProvider makeProvider()
        {
            return new TwoPCProvider();
        }
        
    };
    
    public abstract ConsensusProtocolProvider makeProvider();
}
