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
package org.hit.consensus.raft;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.NodeID;
import org.hit.consensus.UnitID;
import org.hit.messages.ConsensusMessage;

/**
 * Extends {@link ConsensusMessage} to support sending a response to the 
 * {@link RaftReplicationMessage}.
 * 
 * @author Balraja Subbiah
 */
public class RaftReplicationResponse extends ConsensusMessage
{
    private boolean myAccepted;
    
    private long myAcceptedTermID;
    
    private long myAcceptedSeqNo;

    /**
     * CTOR
     */
    public RaftReplicationResponse()
    {
        super();
    }
    
    /**
     * CTOR
     */
    public RaftReplicationResponse(
        NodeID nodeId,
        UnitID unitID)
    {
        this(nodeId, unitID, false, -1L, -1L);
    }

    /**
     * CTOR
     */
    public RaftReplicationResponse(
        NodeID nodeId,
        UnitID unitID,
        boolean accepted,
        long acceptedTermID,
        long acceptedSeqNO)
    {
        super(nodeId, unitID, null);
        myAcceptedTermID = acceptedTermID;
        myAcceptedSeqNo = acceptedSeqNO;
    }

    /**
     * Returns the value of acceptedTermID
     */
    public long getAcceptedTermID()
    {
        return myAcceptedTermID;
    }

    /**
     * Returns the value of acceptedSeqNo
     */
    public long getAcceptedSeqNo()
    {
        return myAcceptedSeqNo;
    }
    
    /**
     * Returns true if the consensus proposal has been accepted by the 
     * other server, else returns false.
     */
    public boolean isAccepted()
    {
        return myAccepted;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myAccepted       = in.readBoolean();
        myAcceptedTermID = in.readLong();
        myAcceptedSeqNo  = in.readLong();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeBoolean(myAccepted);
        out.writeLong(myAcceptedTermID);
        out.writeLong(myAcceptedSeqNo);
    }
}
