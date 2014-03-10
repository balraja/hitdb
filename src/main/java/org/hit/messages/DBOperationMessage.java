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
import org.hit.db.model.DBOperation;
import org.hit.pool.Poolable;

/**
 * Instructs the datbase to apply the given operation to the database.
 *
 * @author Balraja Subbiah
 */
public class DBOperationMessage extends Message implements Poolable
{
    private DBOperation myOperation;

    private long mySequenceNumber;

    /**
     * CTOR
     */
    public DBOperationMessage()
    {
        myOperation = null;
        mySequenceNumber = -1L;
    }

    /**
     * CTOR
     */
    public void initialize(NodeID clientID, long seqNum, DBOperation operation)
    {
        setSenderID(clientID);
        mySequenceNumber = seqNum;
        myOperation = operation;
    }

    /**
     * Returns the value of operation
     */
    public DBOperation getOperation()
    {
        return myOperation;
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
        myOperation = (DBOperation) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeLong(mySequenceNumber);
        out.writeObject(myOperation);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        setSenderID(null);
        myOperation = null;
        mySequenceNumber = -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
    {
    }
}
