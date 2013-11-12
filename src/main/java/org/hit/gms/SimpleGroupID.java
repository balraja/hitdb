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
package org.hit.gms;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Implements {@link GroupID} as a wrapper over the group name.
 * 
 * @author Balraja Subbiah
 */
public class SimpleGroupID implements GroupID
{
    private String myGroupName;
    
    /**
     * CTOR
     */
    public SimpleGroupID()
    {
       this(null);
    }

    /**
     * CTOR
     */
    public SimpleGroupID(String groupName)
    {
        myGroupName = groupName;
    }

    /**
     * Returns the value of groupName
     */
    public String getGroupName()
    {
        return myGroupName;
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
            + ((myGroupName == null) ? 0 : myGroupName.hashCode());
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
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleGroupID other = (SimpleGroupID) obj;
        if (myGroupName == null) {
            if (other.myGroupName != null)
                return false;
        }
        else if (!myGroupName.equals(other.myGroupName))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "SimpleGroupID [myGroupName=" + myGroupName + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myGroupName);
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        myGroupName = in.readUTF();
    }
}
