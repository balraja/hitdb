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
 * Message for denoting that <code>DBOperationFailed</code> on the database.
 * 
 * @author Balraja Subbiah
 */
public class DBOperationFailureMessage extends Message
{
    private DBOperation myDBOperation;
    
    private String myMessage;
    
    private Throwable myException;
    
    /**
     * CTOR
     */
    public DBOperationFailureMessage()
    {
        this(null, null, null, null);
    }
    
    /**
     * CTOR
     */
    public DBOperationFailureMessage(
        NodeID      nodeID,
        DBOperation dBOperation,
        String message)
    {
        this(nodeID, dBOperation, message, null);
    }

    /**
     * CTOR
     */
    public DBOperationFailureMessage(
        NodeID      nodeID,
        DBOperation dBOperation,
        String message,
        Throwable exception)
    {
        super(nodeID);
        myDBOperation = dBOperation;
        myMessage = message;
        myException = exception;
    }

    /**
     * Returns the value of dBOperation
     */
    public DBOperation getDBOperation()
    {
        return myDBOperation;
    }

    /**
     * Returns the value of exception
     */
    public Throwable getException()
    {
        return myException;
    }

    /**
     * Returns the value of message
     */
    public String getMessage()
    {
        return myMessage;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,ClassNotFoundException
    {
        super.readExternal(in);
        myDBOperation = (DBOperation) in.readObject();
        myMessage     = in.readUTF();
        myException   = (Throwable) in.readObject();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myDBOperation);
        out.writeUTF(myMessage);
        out.writeObject(myException);
    }
}
