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
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.messages.ConsensusMessage;
import org.hit.pool.PooledObjects;

/**
 * Extends {@link ConsensusMessage} to notify followers about a new 
 * {@link Proposal}.
 * 
 * @author Balraja Subbiah
 */
public class RaftReplicationMessage extends ConsensusMessage
{
    private long myTermID;
    
    private long mySequenceNumber;
    
    private long myLastCommittedTermID;
    
    private long myLastCommittedSeqNo;
    
    /**
     * CTOR
     */
    public RaftReplicationMessage()
    {
        super();
    }

    /**
     * CTOR
     */
    public static RaftReplicationMessage create(
        NodeID nodeId,
        UnitID unitID,
        Proposal proposal,
        long termID,
        long sequenceNumber,
        long lcTermID,
        long lcSeqNo)
    {
        RaftReplicationMessage replicationMessage = 
            PooledObjects.getInstance(RaftReplicationMessage.class);
        ConsensusMessage.populate(replicationMessage, nodeId, unitID, proposal);
        replicationMessage.myTermID = termID;
        replicationMessage.mySequenceNumber = sequenceNumber;
        replicationMessage.myLastCommittedTermID = lcTermID;
        replicationMessage.myLastCommittedSeqNo = lcSeqNo;
        return replicationMessage;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        super.free();
        myTermID = Long.MIN_VALUE;
        mySequenceNumber = Long.MIN_VALUE;
        myLastCommittedSeqNo = Long.MIN_VALUE;
        myLastCommittedTermID = Long.MIN_VALUE;
    }

    /**
     * Returns the value of termID
     */
    public long getTermID()
    {
        return myTermID;
    }

    /**
     * Returns the value of sequenceNumber
     */
    public long getSequenceNumber()
    {
        return mySequenceNumber;
    }
    
    /**
     * Returns the value of lastCommittedTermID
     */
    public long getLastCommittedTermID()
    {
        return myLastCommittedTermID;
    }

    /**
     * Returns the value of lastCommittedSeqNo
     */
    public long getLastCommittedSeqNo()
    {
        return myLastCommittedSeqNo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myTermID = in.readLong();
        mySequenceNumber = in.readLong();
        myLastCommittedTermID = in.readLong();
        myLastCommittedSeqNo = in.readLong();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeLong(myTermID);
        out.writeLong(mySequenceNumber);
        out.writeLong(myLastCommittedTermID);
        out.writeLong(myLastCommittedSeqNo);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Raft replication message " + myTermID + " : " + mySequenceNumber;
    }
}
