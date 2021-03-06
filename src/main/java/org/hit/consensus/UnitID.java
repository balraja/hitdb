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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.pool.Internable;

/**
 * Defines the contract for uniquely identifying the unit which is guarded by
 * {@link ConsensusProtocol}.
 * 
 * @author Balraja Subbiah
 */
public abstract class UnitID implements Internable
{
    private ConsensusType myConsensusType;

    /**
     * CTOR
     */
    public UnitID()
    {
        this(null);
    }
    
    /**
     * CTOR
     */
    public UnitID(ConsensusType type)
    {
        super();
        myConsensusType = type;
    }

    /**
     * Returns the value of consensusType
     */
    public ConsensusType getConsensusType()
    {
        return myConsensusType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public
        int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
            + ((myConsensusType == null) ? 0 : myConsensusType.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public
        boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        UnitID other = (UnitID) obj;
        if (myConsensusType != other.myConsensusType)
            return false;
        return true;
    }
}
