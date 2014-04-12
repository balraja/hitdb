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

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * The reply message for a successful <code>DBOperation</code>
 *
 * @author Balraja Subbiah
 */
@PoolConfiguration(size=10000,initialSize=100)
public class DBOperationSuccessMessage extends Message implements Poolable
{
    private Object myResult;

    private long mySequenceNumber;

    /**
     * CTOR
     */
    public DBOperationSuccessMessage()
    {
        mySequenceNumber = -1L;
        myResult         = null;
    }

    /**
     * Factory method for creating an instance of 
     * <code>DBOperationSuccessMessage</code> 
     * and populating it with various parameters.
     */
    public static DBOperationSuccessMessage create(
        NodeID serverID, long seqNum, Object result)
    {
        DBOperationSuccessMessage successMessage =
            PooledObjects.getInstance(DBOperationSuccessMessage.class);
        successMessage.setSenderID(null);
        successMessage.mySequenceNumber = seqNum;
        successMessage.myResult         = result;
        return successMessage;
    }

    /**
     * Returns the value of result
     */
    public Object getResult()
    {
        return myResult;
    }

    /**
     * Returns the value of sequenceNumber
     */
    public long getSequenceNumber()
    {
        return mySequenceNumber;
    }
    
    /**
     * Setter for result
     */
    public void setResult(Object result)
    {
        myResult = result;
    }

    /**
     * Setter for sequenceNumber
     */
    public void setSequenceNumber(long sequenceNumber)
    {
        mySequenceNumber = sequenceNumber;
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
        boolean hasResult = in.readBoolean();
        if (hasResult) {
            myResult = in.readObject();
        }
        else {
            myResult = null;
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
        if (myResult != null) {
            out.writeBoolean(true);
            out.writeObject(myResult);
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
        myResult         = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
    {
    }
}
