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

package org.hit.communicator;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.event.Event;

/**
 * Defines the contract for a Message to be sent on a wire.
 * 
 * @author Balraja Subbiah
 */
public abstract class Message implements Externalizable, Event
{
    private NodeID mySenderId;
    
    /**
     * CTOR
     */
    public Message()
    {
        mySenderId = null;
    }

    /**
     * CTOR
     */
    public Message(NodeID senderId)
    {
        mySenderId = senderId;
    }
    
    /**
     * Returns the value of nodeId corresponding to the originating node of
     * this message.
     */
    public NodeID getSenderId()
    {
        return mySenderId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
    {
        mySenderId = (NodeID) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(mySenderId);
    }
}
