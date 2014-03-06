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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;

import org.hit.communicator.NodeID;
import org.hit.pool.Internable;
import org.hit.pool.InternedBy;

import com.google.common.net.InetAddresses;

/**
 * An implementation of {@link NodeID} where each node is uniquely identified
 * by it's IPAddress.
 * 
 * @author Balraja Subbiah
 */
@InternedBy(interner=IPNodeIDInterner.class);
public class IPNodeID implements NodeID,Internable
{

    public static final String SEPARATOR = ":";
    
    private InetSocketAddress myIPAddress;
    
    /**
     * CTOR
     */
    public IPNodeID()
    {
        myIPAddress = null;
    }

    
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
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return InetAddresses.toAddrString(myIPAddress.getAddress())
               + SEPARATOR
               + Integer.toString(myIPAddress.getPort());
    }
    
    /** Parses the string representation of the nodeId */
    protected static InetSocketAddress parseAddress(String value)
    {
        String[] hostAndPort = value.split(SEPARATOR);
        InetSocketAddress address = 
            new InetSocketAddress(
                InetAddresses.forString(hostAndPort[0]),
                Integer.parseInt(hostAndPort[1]));
        return address;
    }
}
