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

package org.hit.transactions.test;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.NodeID;

/**
 * Extends {@link NodeID} to support testing.
 * 
 * @author Balraja Subbiah
 */
public class TestID implements NodeID
{
    public static final TestID SERVER_NODE_ID = new TestID("SERVER");
    
    public static final TestID CLIENT_NODE_ID = new TestID("CLIENT");
    
    private String myID;

    /**
     * CTOR
     */
    public TestID(String iD)
    {
        super();
        myID = iD;
    }

    /**
     * Returns the value of iD
     */
    public String getID()
    {
        return myID;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myID = in.readUTF();        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myID);        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((myID == null) ? 0 : myID.hashCode());
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
        TestID other = (TestID) obj;
        if (myID == null) {
            if (other.myID != null)
                return false;
        }
        else if (!myID.equals(other.myID))
            return false;
        return true;
    }
}
