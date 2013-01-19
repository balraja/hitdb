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
import org.hit.consensus.ConsensusLeader;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.event.ConsensusMessage;

/**
 * The message that's sent from  {@link ConsensusAcceptor} to
 * {@link ConsensusLeader} in response to leader's
 * {@link PaxosSolicitConsensusMessage}.
 * 
 * @author Balraja Subbiah
 */
public class PaxosConsensusAcceptMessage extends ConsensusMessage
{
    private final long myAcceptedID;
    
    private boolean isAccepted;
    
    private long myPreviousAcceptedID;
    
    private Proposal myPreviouslyAcceptedProposal;

    /**
     * CTOR
     */
    public PaxosConsensusAcceptMessage(
       NodeID nodeID,
       UnitID unitID,
       long acceptedID,
       boolean accepted,
       long previousAcceptID,
       Proposal previousProposal)
    {
        super(nodeID, unitID);
        myAcceptedID = acceptedID;
        isAccepted = accepted;
        myPreviousAcceptedID = previousAcceptID;
        myPreviouslyAcceptedProposal = previousProposal;
    }

    /**
     * Returns the value of acceptedID
     */
    public long getAcceptedID()
    {
        return myAcceptedID;
    }

    /**
     * Returns the value of previousAcceptedID
     */
    public long getPreviousAcceptedID()
    {
        return myPreviousAcceptedID;
    }
    
    /**
     * Returns the value of previouslyAcceptedProposal
     */
    public Proposal getPreviouslyAcceptedProposal()
    {
        return myPreviouslyAcceptedProposal;
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
        myPreviousAcceptedID = in.readLong();
        myPreviouslyAcceptedProposal =
            (Proposal) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeBoolean(isAccepted);
        out.writeLong(myPreviousAcceptedID);
        out.writeObject(myPreviouslyAcceptedProposal);
    }
}
