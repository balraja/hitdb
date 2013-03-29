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
import java.util.Map;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;

/**
 * Defines the contract for the Message being sent to the nodes for 
 * performing operations across multitude of nodes. This message is primarily
 * sent to the leader which in turns coordinates the execution across all 
 * transactions.
 * 
 * @author Balraja Subbiah
 */
public class DistributedDBOperationMessage extends Message
{
    private Map<NodeID, DBOperation> myNodeToOperationMap;
    
    /**
     * CTOR
     */
    public DistributedDBOperationMessage()
    {
        myNodeToOperationMap = null;
    }

    /**
     * CTOR
     */
    public DistributedDBOperationMessage(
        NodeID clientId,
        Map<NodeID, DBOperation> nodeToOperationMap)
    {
        super(clientId);
        myNodeToOperationMap = nodeToOperationMap;
    }

    /**
     * Returns the value of nodeToOperationMap
     */
    public Map<NodeID, DBOperation> getNodeToOperationMap()
    {
        return myNodeToOperationMap;
    }
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myNodeToOperationMap = (Map<NodeID, DBOperation>) in.readObject();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myNodeToOperationMap);
    }
}
