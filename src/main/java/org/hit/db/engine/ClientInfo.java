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

import org.hit.communicator.NodeID;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * A simple class to implement a structure for capturing the 
 * client information.
 */
public class ClientInfo implements Poolable
{
    private NodeID myClientID;
    
    private long myClientSequenceNumber;

    /**
     * Factory method for creating an instance of <code>ClientInfo</code> 
     * and populating with various parameters.
     */
    public static ClientInfo create(NodeID clientID, long clientSequenceNumber)
    {
        ClientInfo clientInfo = PooledObjects.getInstance(ClientInfo.class);
        clientInfo.myClientID = clientID;
        clientInfo.myClientSequenceNumber = clientSequenceNumber;
        return clientInfo;
    }

    /**
     * Returns the value of clientID
     */
    public NodeID getClientID()
    {
        return myClientID;
    }

    /**
     * Returns the value of clientSequenceNumber
     */
    public long getClientSequenceNumber()
    {
        return myClientSequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myClientID = null;
        myClientSequenceNumber = Long.MIN_VALUE;
    }
}