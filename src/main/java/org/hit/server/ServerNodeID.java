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
package org.hit.server;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;

import org.hit.communicator.nio.IPNodeID;

/**
 * Extends {@link IPNodeID} to include the server name.
 * 
 * @author Balraja Subbiah
 */
public class ServerNodeID extends IPNodeID
{
    private String myName;

    /**
     * CTOR
     */
    public ServerNodeID()
    {
        this(null, null);
    }

    /**
     * CTOR
     */
    public ServerNodeID(int bindingPort, String name)
    {
        super(bindingPort);
        myName = name;
    }
    
    /**
     * CTOR
     */
    public ServerNodeID(String representation)
    {
        this(
            parseAddress(
                representation.substring(
                    representation.indexOf(SEPARATOR) + 1)),
            representation.substring(0, representation.indexOf(SEPARATOR)));
    }
    
    /**
     * CTOR
     */
    public ServerNodeID(InetSocketAddress address, String name)
    {
        super(address);
        myName = name;
    }


    /**
     * Returns the value of name
     */
    public String getName()
    {
        return myName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((myName == null) ? 0 : myName.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        ServerNodeID other = (ServerNodeID) obj;
        if (myName == null) {
            if (other.myName != null)
                return false;
        }
        else if (!myName.equals(other.myName))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return myName + SEPARATOR + super.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
            throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myName = in.readUTF();;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeUTF(myName);
    }
}
