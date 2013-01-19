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

package org.hit.communicator.nio;

import java.net.InetSocketAddress;

import org.hit.communicator.NodeID;

/**
 * An implementation of {@link NodeID} where each node is uniquely identified
 * by it's IPAddress.
 * 
 * @author Balraja Subbiah
 */
public class IPNodeID implements NodeID
{
    private final InetSocketAddress myIPAddress;
    
    /**
     * CTOR
     */
    public IPNodeID(InetSocketAddress iPAddress)
    {
        myIPAddress = iPAddress;
    }

    /**
     * CTOR
     */
    public IPNodeID(int bindingPort)
    {
        myIPAddress = new InetSocketAddress(bindingPort);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        IPNodeID other = (IPNodeID) obj;
        if (myIPAddress == null) {
            if (other.myIPAddress != null) {
                return false;
            }
        }
        else if (!myIPAddress.equals(other.myIPAddress)) {
            return false;
        }
        return true;
    }

    /**
     * Returns the value of iPAddress
     */
    public InetSocketAddress getIPAddress()
    {
        return myIPAddress;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                 + ((myIPAddress == null) ? 0 : myIPAddress.hashCode());
        return result;
    }
}
