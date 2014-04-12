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
import org.hit.pool.PooledObjects;

/**
 * The {@link ConsensusMessage} that's being sent to initiate the commit.
 * 
 * @author Balraja Subbiah
 */
public class CommitRequest extends ConsensusMessage
{
    private boolean myShouldCommit;
    
    /**
     * CTOR
     */
    public CommitRequest()
    {
        super();
    }

    /**
     * CTOR
     */
    public static CommitRequest create(NodeID nodeId, 
                                       UnitID unitID, 
                                       Proposal proposal,
                                       boolean commit)
    {
        CommitRequest cr = PooledObjects.getInstance(CommitRequest.class);
        ConsensusMessage.populate(cr, nodeId, unitID, proposal);
        cr.myShouldCommit = commit;
        return cr;
    }

    /**
     * Returns the value of shouldCommit
     */
    public boolean shouldCommit()
    {
        return myShouldCommit;
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
        myShouldCommit = in.readBoolean();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public
        void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeBoolean(myShouldCommit);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        super.free();
        myShouldCommit = false;
    }
}
