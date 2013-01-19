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

package org.hit.event;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.hms.SuspectVector;

/**
 * Defines the <code>Message</code> that's exchanged among the nodes for
 * sharing gossip information.
 * 
 * @author Balraja Subbiah
 */
public class GossipMessage extends Message
{
    private SuspectVector mySuspectVector;

    /**
     * CTOR
     */
    public GossipMessage()
    {
        super();
    }

    /**
     * CTOR
     */
    public GossipMessage(NodeID nodeId, SuspectVector suspectVector)
    {
        super(nodeId);
        mySuspectVector = suspectVector;
    }

    /**
     * Returns the value of suspectVector
     */
    public SuspectVector getSuspectVector()
    {
        return mySuspectVector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        mySuspectVector = (SuspectVector) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(mySuspectVector);
    }
}
