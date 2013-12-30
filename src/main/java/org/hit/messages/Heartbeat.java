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

package org.hit.messages;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;

/**
 * Defines the heart beat to be published by client nodes to the master.
 * 
 * @author Balraja Subbiah
 */
public class Heartbeat extends Message
{
    private TObjectLongHashMap<String> myTableToRowCountMap;
    
    /**
     * CTOR
     */
    public Heartbeat()
    {
        this(null, null);
    }

    /**
     * CTOR
     */
    public Heartbeat(NodeID from, TObjectLongMap<String> tableRowCountMap)
    {
        super(from);
        myTableToRowCountMap = 
            tableRowCountMap != null ? 
                new TObjectLongHashMap<>(tableRowCountMap) : null;
    }

    /**
     * Returns the value of tableToRowCountMap
     */
    public TObjectLongHashMap<String> getTableToRowCountMap()
    {
        return myTableToRowCountMap;
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
    {
        super.readExternal(in);
        if (in.readBoolean()) {
            myTableToRowCountMap = (TObjectLongHashMap<String>) in.readObject();
        }
        else {
            myTableToRowCountMap = null;
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        if (myTableToRowCountMap != null) {
            out.writeBoolean(true);
            out.writeObject(myTableToRowCountMap);
        }
        else {
            out.writeBoolean(false);
        }
    }
}
