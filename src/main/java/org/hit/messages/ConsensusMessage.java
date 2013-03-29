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

package org.hit.messages;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.consensus.UnitID;

/**
 * A marker interface that defines the characteristics of messages used in a
 * consensus.
 * 
 * @author Balraja Subbiah
 */
public abstract class ConsensusMessage extends Message
{
    /**
     * The id of the unit that's guarded by a consensus protocol for which
     * this message is intended.
     */
    private UnitID myUnitID;

    /**
     * CTOR
     */
    public ConsensusMessage()
    {
        super();
        myUnitID = null;
    }

    /**
     * CTOR
     */
    public ConsensusMessage(NodeID nodeId, UnitID unitID)
    {
        super(nodeId);
        myUnitID = unitID;
    }
    
    /**
     * Returns the value of unitID
     */
    public UnitID getUnitID()
    {
        return myUnitID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myUnitID = (UnitID) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myUnitID);
    }
}
