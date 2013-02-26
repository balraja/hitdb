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

package org.hit.distribution.test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import org.hit.communicator.NodeID;

/**
 * Implements <code>NodeID</code> to use unique strings for testing purposes.
 * 
 * @author Balraja Subbiah
 */
public class StringNodeID implements NodeID
{
    private String myName;
    
    /**
     * CTOR
     */
    public StringNodeID()
    {
        myName = null;
    }

    /**
     * CTOR
     */
    public StringNodeID(String name)
    {
        myName = name;
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
        StringNodeID other = (StringNodeID) obj;
        if (myName == null) {
            if (other.myName != null) {
                return false;
            }
        }
        else if (!myName.equals(other.myName)) {
            return false;
        }
        return true;
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
        int result = 1;
        result = prime * result + ((myName == null) ? 0 : myName.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "NodeID [" + myName + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myName = in.readUTF();
    }
}
