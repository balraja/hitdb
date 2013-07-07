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
import org.hit.partitioner.Partitioner;

/**
 * Defines the response from the batabase server to the client for later's
 * request to create a new table.
 *
 * @author Balraja Subbiah
 */
public class CreateTableResponseMessage extends Message
{
    private String myErrorMessage;

    private Partitioner<?,?> myPartitioner;

    private String myTableName;

    /**
     * CTOR
     */
    public CreateTableResponseMessage()
    {
        this(null, null, null, null);
    }

    /**
     * CTOR
     */
    public CreateTableResponseMessage(NodeID  nodeId,
                                      String  tableName,
                                      Partitioner<?,?> partitioner,
                                      String  errorMessage)
    {
        super(nodeId);
        myTableName = tableName;
        myPartitioner = partitioner;
        myErrorMessage = errorMessage;
    }

    /**
     * Returns the value of errorMessage
     */
    public String getErrorMessage()
    {
        return myErrorMessage;
    }

    /**
     * Returns the value of isSuccessful
     */
    public Partitioner<?,?> getPartitioner()
    {
        return myPartitioner;
    }

    /**
     * Returns the value of tableName
     */
    public String getTableName()
    {
        return myTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myTableName = in.readUTF();
        myPartitioner = (Partitioner<?, ?>) in.readObject();
        boolean hasError = in.readBoolean();
        if (hasError) {
            myErrorMessage = in.readUTF();
        }
        else {
            myErrorMessage = null;
        }
    }

    /**
     * Setter for the errorMessage
     */
    public void setErrorMessage(String errorMessage)
    {
        myErrorMessage = errorMessage;
    }

    /**
     * Setter for the isSuccessful
     */
    public void setPartitioner(Partitioner<?,?> partitioner)
    {
        myPartitioner = partitioner;
    }

    /**
     * Setter for the tableName
     */
    public void setTableName(String tableName)
    {
        myTableName = tableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeUTF(myTableName);
        out.writeObject(myPartitioner);
        if (myErrorMessage != null) {
            out.writeBoolean(true);
            out.writeUTF(myErrorMessage);
        }
        else {
            out.writeBoolean(false);
        }
    }
}
