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
import java.util.HashMap;
import java.util.Map;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.pool.Poolable;

/**
 * Defines the contract for the Message being sent to the nodes for 
 * performing operations across multitude of nodes. This message is primarily
 * sent to the leader which in turns coordinates the execution across all 
 * other nodes.
 * 
 * @author Balraja Subbiah
 */
public class DistributedDBOperationMessage extends Message 
    implements Poolable
{
    private long                           mySequenceNumber;
    
    private final Map<NodeID, DBOperation> myNodeToOperationMap;
    
    /**
     * CTOR
     */
    public DistributedDBOperationMessage()
    {
        mySequenceNumber     = -1L;
        myNodeToOperationMap = new HashMap<>();
    }

    /**
     * CTOR
     */
    public DistributedDBOperationMessage initialize(
        NodeID                   clientId,
        long                     sequenceNumber,
        Map<NodeID, DBOperation> nodeToOperationMap)
    {
        setSenderID(clientId);
        mySequenceNumber     = sequenceNumber;
        myNodeToOperationMap.putAll(nodeToOperationMap);
        return this;
    }

    /**
     * Returns the value of nodeToOperationMap
     */
    public Map<NodeID, DBOperation> getNodeToOperationMap()
    {
        return myNodeToOperationMap;
    }
    
    /**
     * Returns the value of sequenceNumber
     */
    public long getSequenceNumber()
    {
        return mySequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        mySequenceNumber = in.readLong();
        boolean hasElements = in.readBoolean();
        if (hasElements) {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                NodeID nodeID = (NodeID) in.readObject();
                DBOperation operation = (DBOperation) in.readObject();
                myNodeToOperationMap.put(nodeID, operation);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeLong(mySequenceNumber);
        if (!myNodeToOperationMap.isEmpty()) {
            out.writeBoolean(true);
            out.writeInt(myNodeToOperationMap.size());
            // We still end up creating Entry objects and iterator
            // as garbage.
            for (Map.Entry<NodeID, DBOperation> entry : 
                    myNodeToOperationMap.entrySet())
            {
                out.writeObject(entry.getKey());
                out.writeObject(entry.getValue());
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
        mySequenceNumber = -1L;
        myNodeToOperationMap.clear();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
    {
    }
}
