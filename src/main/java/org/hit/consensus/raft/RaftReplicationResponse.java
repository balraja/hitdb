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
        UnitID unitID,
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
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
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
        out.writeLong(myAcceptedTermID);
        out.writeLong(myAcceptedSeqNo);
    }
}
