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
import org.hit.db.partitioner.Partitioner;
import org.hit.db.partitioner.TablePartitionInfo;

/**
 * Defines the contract for {@link Message} that's sent in response to 
 * {@link FacadeInitRequest}.
 * 
 * @author Balraja Subbiah
 */
public class FacadeInitResponse extends Message
{
    private TablePartitionInfo myPartitionInfo;

    /**
     * CTOR
     */
    public FacadeInitResponse()
    {
        super();
    }

    /**
     * CTOR
     */
    public FacadeInitResponse(NodeID nodeId, TablePartitionInfo partitionInfo)
    {
        super(nodeId);
        myPartitionInfo = partitionInfo;
    }

    /**
     * Returns the value of partitions
     */
    public TablePartitionInfo getPartitionInfo()
    {
        return myPartitionInfo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myPartitionInfo = (TablePartitionInfo) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myPartitionInfo);
    }
}
