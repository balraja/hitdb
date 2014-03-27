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

package org.hit.example;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;

import org.hit.db.model.Persistable;
import org.hit.db.model.mutations.MutationFactory;
import org.hit.pool.Copyable;
import org.hit.pool.PooledObjects;

import com.google.common.collect.Lists;

/**
 * Defines the contract for the table that holds information about an airline
 * 
 * @author Balraja Subbiah
 */
public class Airline implements Persistable<Long>, Externalizable
{
    public static final String TABLE_NAME = "airlines";
    
    private static final String AIRLINE_ID   = "airline_id";
    
    private static final String AIRLINE_NAME = "name";
    
    private long myID;
    
    private String myName;

    /**
     * CTOR
     */
    public Airline()
    {
        super();
    }

    /**
     * CTOR
     */
    public Airline initialize(long iD, String name)
    {
        myID = iD;
        myName = name;
        return this;
    }

    /**
     * Returns the value of iD
     */
    public long getID()
    {
        return myID;
    }

    /**
     * Setter for the iD
     */
    public void setID(long iD)
    {
        myID = iD;
    }

    /**
     * Returns the value of name
     */
    public String getName()
    {
        return myName;
    }

    /**
     * Setter for the name
     */
    public void setName(String name)
    {
        myName = name;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFieldValue(String fieldName)
    {
        switch(fieldName) {
        case AIRLINE_ID:
            return getID();
        case AIRLINE_NAME:
            return getName();
        default:
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(myID);
        out.writeUTF(myName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
        myID = in.readLong();
        myName = in.readUTF();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long primaryKey()
    {
        return Long.valueOf(myID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (myID ^ (myID >>> 32));
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
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Airline other = (Airline) obj;
        if (myID != other.myID)
            return false;
        if (myName == null) {
            if (other.myName != null)
                return false;
        }
        else if (!myName.equals(other.myName))
            return false;
        return true;
    }
    
    public static MutationFactory<Long,Airline> getMutationFactory()
    {
        return new MutationFactory<>(TABLE_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<String> getFieldNames()
    {
        return Lists.newArrayList(AIRLINE_ID, AIRLINE_NAME);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myID = Long.MIN_VALUE;
        myName = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Airline getCopy()
    {
        return PooledObjects.getInstance(Airline.class)
                            .initialize(myID, myName);
    }
}
