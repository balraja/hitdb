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

import org.hit.communicator.NodeID;
import org.hit.consensus.UnitID;

/**
 * Defines an <code>UnitID</code> for replicated databases.
 * 
 * @author Balraja Subbiah
 */
public class ReplicatedDatabaseID implements UnitID
{
    private final NodeID myNodeID;

    /**
     * CTOR
     */
    public ReplicatedDatabaseID(NodeID nodeID)
    {
        super();
        myNodeID = nodeID;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ReplicatedDatabaseID other = (ReplicatedDatabaseID) obj;
        if (myNodeID == null) {
            if (other.myNodeID != null) {
                return false;
            }
        }
        else if (!myNodeID.equals(other.myNodeID)) {
            return false;
        }
        return true;
    }



    /**
     * Returns the value of nodeID
     */
    public NodeID getNodeID()
    {
        return myNodeID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                 + ((myNodeID == null) ? 0 : myNodeID.hashCode());
        return result;
    }
}
