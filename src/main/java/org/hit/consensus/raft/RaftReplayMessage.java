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

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.NodeID;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.messages.ConsensusMessage;

/**
 * Extends {@link ConsensusMessage} to support replaying missing messages
 * in the WAL to {@link RaftAcceptor}s.
 * 
 * @author Balraja Subbiah
 */
public class RaftReplayMessage extends ConsensusMessage
{
    private long myTermID;
    
    private TLongObjectMap<Proposal> myReplayedProposals;
    
    /**
     * CTOR
     */
    public RaftReplayMessage()
    {
        myReplayedProposals = new TLongObjectHashMap<>();
    }

    /**
     * CTOR
     */
    public RaftReplayMessage(NodeID senderID,
                             UnitID unitID,
                             long termID, 
                             TLongObjectMap<Proposal> replayedProposals)
    {
        setSenderID(senderID);
        setUnitID(unitID);
        myTermID = termID;
        myReplayedProposals = replayedProposals;
    }

    /**
     * Returns the value of termID
     */
    public long getTermID()
    {
        return myTermID;
    }

    /**
     * Setter for termID
     */
    public void setTermID(long termID)
    {
        myTermID = termID;
    }

    /**
     * Returns the value of replayedProposals
     */
    public TLongObjectMap<Proposal> getReplayedProposals()
    {
        return myReplayedProposals;
    }

    /**
     * Setter for replayedProposals
     */
    public void setReplayedProposals(TLongObjectMap<Proposal> replayedProposals)
    {
        myReplayedProposals = replayedProposals;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long key = in.readLong();
            Proposal value = (Proposal) in.readObject();
            myReplayedProposals.put(key, value);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeInt(myReplayedProposals.size());
        TLongObjectIterator<Proposal> itr = myReplayedProposals.iterator();
        while (itr.hasNext()) {
            out.writeLong(itr.key());
            out.writeObject(itr.value());
        }
    }
    
    
    
}
