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
package org.hit.db.engine;

import org.hit.consensus.ConsensusType;
import org.hit.consensus.UnitID;

/**
 * Extends {@link UnitID} to create an unique id for replication.
 * 
 * @author Balraja Subbiah
 */
public class ConsensusID extends UnitID
{
    public static final String DELIMITER = ":";
    
    private String myServerName;

    /**
     * CTOR
     */
    public ConsensusID()
    {
        super();
    }

    /**
     * CTOR
     */
    public ConsensusID(ConsensusType type, String serverName)
    {
        super(type);
        myServerName = serverName;
    }

    /**
     * Returns the value of serverName
     */
    public String getServerName()
    {
        return myServerName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result
            + ((myServerName == null) ? 0 : myServerName.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConsensusID other = (ConsensusID) obj;
        if (myServerName == null) {
            if (other.myServerName != null)
                return false;
        }
        else if (!myServerName.equals(other.myServerName))
            return false;
        return true;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return getConsensusType().name() + " " + myServerName;
    }
}
