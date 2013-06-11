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

/**
 * Instructs the datbase to apply the given operation to the database.
 *
 * @author Balraja Subbiah
 */
public class DBOperationMessage extends Message
{
    private DBOperation myOperation;

    /**
     * CTOR
     */
    public DBOperationMessage()
    {
        this(null, null);
    }

    /**
     * CTOR
     */
    public DBOperationMessage(NodeID clientID, DBOperation operation)
    {
        super(clientID);
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
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myOperation = (DBOperation) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myOperation);
    }
}