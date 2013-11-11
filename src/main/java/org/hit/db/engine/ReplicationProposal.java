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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.db.model.Mutation;

/**
 * Extends {@link Proposal} to support sending @link {Mutation} to 
 * other nodes for replication.
 * 
 * @author Balraja Subbiah
 */
public class ReplicationProposal implements Proposal
{
    private UnitID myUnitID;
    
    private Mutation myMutation;
    
    private long myStart;
    
    private long myEndTime;

    /**
     * CTOR
     */
    public ReplicationProposal()
    {
        this(null, null, -1L, -1L);
    }
    
    /**
     * CTOR
     */
    public ReplicationProposal(
        UnitID unitID, Mutation mutation, long start, long endTime)
    {
        super();
        myUnitID = unitID; 
        myMutation = mutation;
        myStart = start;
        myEndTime = endTime;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myUnitID);
        out.writeObject(myMutation);
        out.writeLong(myStart);
        out.writeLong(myEndTime);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        myUnitID = (UnitID) in.readObject();
        myMutation = (Mutation) in.readObject();
        myStart = in.readLong();
        myEndTime = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UnitID getUnitID()
    {
        return myUnitID;
    }

}
