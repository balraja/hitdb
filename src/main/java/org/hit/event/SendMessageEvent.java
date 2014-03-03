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

import java.util.ArrayList;
import java.util.Collection;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.pool.PoolUtils;
import org.hit.pool.Poolable;

/**
 * Defines the contract for an event that initiates the sending to other
 * database nodes.
 * 
 * @author Balraja Subbiah
 */
public class SendMessageEvent implements Event, Poolable
{
    private final Collection<NodeID> myTargets;
    
    private Message myMessage;
    
    /**
     * CTOR
     */
    public SendMessageEvent()
    {
        myTargets = new ArrayList<>();
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        PoolUtils.free(myTargets);
        // XXX release message also.
        myTargets.clear();
        myMessage = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
    {
        myMessage = null;
    }
    
    /**
     * Initializes the instance with the required attributes.
     */
    public SendMessageEvent initialize(NodeID target, Message message)
    {
        myTargets.add(target);
        myMessage = message;
        return this;
    }

    
    /**
     * Initializes the instance with the required attributes.
     */
    public SendMessageEvent initialize(Collection<NodeID> targets, 
                                       Message            message)
    {
        myTargets.addAll(targets);
        myMessage = message;
        return this;
    }

}
