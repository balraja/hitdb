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
package org.hit.consensus.twopc;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.NodeID;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.messages.ConsensusMessage;

/**
 * Extends <code>ConsensusMessage</code> to support accepting consensus.
 * 
 * @author Balraja Subbiah
 */
public class ConsensusAcceptMessage extends ConsensusMessage
{
    private boolean myAccepted;

    /**
     * CTOR
     */
    public ConsensusAcceptMessage()
    {
        super();
    }

    /**
     * CTOR
     */
    public ConsensusAcceptMessage initialize(
        NodeID nodeId,
        UnitID unitID,
        Proposal proposal,
        boolean accepted)
    {
        super.initialize(nodeId, unitID, proposal);
        myAccepted = accepted;
        return this;
    }

    /**
     * Returns the value of accepted
     */
    public boolean isAccepted()
    {
        return myAccepted;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public
        void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myAccepted = in.readBoolean();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public
        void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeBoolean(myAccepted);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        super.free();
        myAccepted = false;
    }
}
