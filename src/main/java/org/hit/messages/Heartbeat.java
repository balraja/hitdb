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

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines the heart beat to be published by client nodes to the master.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(initialSize = 5, size = 10)
public class Heartbeat extends Message implements Poolable
{
    private final TObjectLongHashMap<String> myTableToRowCountMap;
    
    /**
     * CTOR
     */
    public Heartbeat()
    {
        myTableToRowCountMap = new TObjectLongHashMap<>();
    }

    /**
     * Factory method for creating an instance of <code>Heartbeat</code> 
     * and populating with various parameters.
     */
    public static Heartbeat create(NodeID from, 
                                   TObjectLongMap<String> tableRowCountMap)
    {
        Heartbeat heartbeat = PooledObjects.getInstance(Heartbeat.class);
        heartbeat.setSenderID(from);
        Object[] keys = tableRowCountMap.keys();
        long[] values = tableRowCountMap.values();
        for (int i = 0; i < tableRowCountMap.size(); i++) {
            heartbeat.myTableToRowCountMap.put(keys[i].toString(), values[i]);
        }
        return heartbeat;
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
    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
    {
        super.readExternal(in);
        if (in.readBoolean()) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                String key = in.readUTF();
                long value = in.readLong();
                myTableToRowCountMap.put(key, value);
            }
        }
        else {
            myTableToRowCountMap.clear();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        if (!myTableToRowCountMap.isEmpty()) {
            out.writeBoolean(true);
            out.writeInt(myTableToRowCountMap.size());
            Object[] keys = myTableToRowCountMap.keys();
            long[] values = myTableToRowCountMap.values();
            for (int i = 0; i < myTableToRowCountMap.size(); i++) {
                out.writeUTF(keys[i].toString());
                out.writeLong(values[i]);
            }
        }
        else {
            out.writeBoolean(false);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        setSenderID(null);
        myTableToRowCountMap.clear();
    }
}
