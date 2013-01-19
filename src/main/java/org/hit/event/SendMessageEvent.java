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

import java.util.Collection;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;

/**
 * Defines the contract for an event that initiates the sending to other
 * database nodes.
 * 
 * @author Balraja Subbiah
 */
public class SendMessageEvent implements Event
{
    private final Collection<NodeID> myTargets;
    
    private final Message myMessage;

    /**
     * CTOR
     */
    public SendMessageEvent(Collection<NodeID> targets, Message message)
    {
        super();
        myTargets = targets;
        myMessage = message;
    }

    /**
     * Returns the value of message
     */
    public Message getMessage()
    {
        return myMessage;
    }

    /**
     * Returns the set of nodes to which the given message is to be sent.
     */
    public Collection<NodeID> getTargets()
    {
        return myTargets;
    }
}
