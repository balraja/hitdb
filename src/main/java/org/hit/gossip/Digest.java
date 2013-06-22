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

package org.hit.gossip;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

/**
 * The information digest that's shared between the partcipants during
 * information dissemination.
 * 
 * @author Balraja Subbiah
 */
public class Digest implements Externalizable
{
    private TObjectLongMap<Serializable> myKeyToVersionMap;
    
    /**
     * CTOR
     */
    public Digest()
    {
        myKeyToVersionMap = new TObjectLongHashMap<>();
    }

    /**
     * CTOR
     */
    public Digest(TObjectLongMap<Serializable> keyToVersionMap)
    {
        myKeyToVersionMap = keyToVersionMap;
    }

    /**
     * Returns the value of keyToVersionMap
     */
    public TObjectLongMap<Serializable> getKeyToVersionMap()
    {
        return myKeyToVersionMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myKeyToVersionMap);        
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
        myKeyToVersionMap = (TObjectLongMap<Serializable>) in.readObject();
    }
}
