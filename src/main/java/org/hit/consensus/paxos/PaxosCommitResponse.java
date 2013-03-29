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

package org.hit.consensus.paxos;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusAcceptor;
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.UnitID;
import org.hit.messages.ConsensusMessage;

/**
 * The {@link ConsensusMessage} sent from {@link ConsensusAcceptor}
 * to {@link ConsensusLeader} sent in response to the {@link PaxosCommitRequest}.
 * 
 * @author Balraja Subbiah
 */
public class PaxosCommitResponse extends ConsensusMessage
{
    private boolean isAccepted;
    
    private long mySequenceID;

    /**
     * CTOR
     */
    public PaxosCommitResponse(NodeID  nodeID,
                               UnitID  unitID,
                               boolean accepted,
                               long    sequenceID)
    {
        super(nodeID, unitID);
        isAccepted = accepted;
        mySequenceID = sequenceID;
    }
    
    /**
     * Returns the value of sequenceID
     */
    public long getSequenceID()
    {
        return mySequenceID;
    }

    /**
     * Returns the value of isAccepted
     */
    public boolean isAccepted()
    {
        return isAccepted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        isAccepted = in.readBoolean();
        mySequenceID = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeBoolean(isAccepted);
        out.writeLong(mySequenceID);
    }
}
