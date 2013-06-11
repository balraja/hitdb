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

package org.hit.messages;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.broadcast.Digest;
import org.hit.communicator.Message;
import org.hit.communicator.NodeID;

/**
 * The message that can be used for sharing digests between nodes.  
 * 
 * @author Balraja Subbiah
 */
public class ReconcillationRequest extends Message
{
    private Digest myDigest;

    /**
     * CTOR
     */
    public ReconcillationRequest()
    {
        super();
    }

    /**
     * CTOR
     */
    public ReconcillationRequest(NodeID nodeId, Digest digest)
    {
        super(nodeId);
        myDigest = digest;
    }

    /**
     * Returns the value of digest
     */
    public Digest getDigest()
    {
        return myDigest;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
        super.readExternal(in);
        myDigest = (Digest) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myDigest);
    }
}
