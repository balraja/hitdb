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

package org.hit.broadcast;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * Defines the contract for an interface that defines the characteristics 
 * of shared information.
 * 
 * @author Balraja Subbiah
 */
public class Information implements Externalizable
{
    /**
     * The key that uniquely identifies the information.
     */
    private Serializable myKey;
    
    /**
     * The value of this key.
     */
    private Serializable myValue;
    
    /**
     * Returns the timestamp at which this information is last updated.
     * We are relying on the system time and obviously we can be bitten by 
     * clock drift.
     */
    private long myTimestamp;

    /**
     * CTOR
     */
    public Information()
    {
        super();
    }

    /**
     * CTOR
     */
    public Information(Serializable key, Serializable value, long timestamp)
    {
        super();
        myKey = key;
        myValue = value;
        myTimestamp = timestamp;
    }

    /**
     * Returns the value of key
     */
    public Serializable getKey()
    {
        return myKey;
    }

    /**
     * Returns the value of value
     */
    public Serializable getValue()
    {
        return myValue;
    }

    /**
     * Returns the value of timestamp
     */
    public long getTimestamp()
    {
        return myTimestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myKey);
        out.writeObject(myValue);
        out.writeLong(myTimestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myKey = (Serializable) in.readObject();
        myValue = (Serializable) in.readObject();
        myTimestamp = in.readLong();
    }
}
