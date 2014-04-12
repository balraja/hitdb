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
import org.hit.pool.PoolConfiguration;
import org.hit.pool.PoolUtils;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines the contract for an event that initiates the sending to other
 * database nodes.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(initialSize = 100, size = 10000)
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
     * Initializes the instance with the required attributes.
     */
    public static SendMessageEvent create(NodeID target, Message message)
    {
        SendMessageEvent sme = PooledObjects.getInstance(SendMessageEvent.class);
        sme.myTargets.add(target);
        sme.myMessage = message;
        return sme;
    }

    
    /**
     * Initializes the instance with the required attributes.
     */
    public static SendMessageEvent create(Collection<NodeID> targets, 
                                          Message            message)
    {
        SendMessageEvent sme = PooledObjects.getInstance(SendMessageEvent.class);
        sme.myTargets.addAll(targets);
        sme.myMessage = message;
        return sme;
    }

}
