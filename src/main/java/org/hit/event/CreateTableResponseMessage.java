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

package org.hit.event;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;

/**
 * Defines the response from the batabase server to the client for later's 
 * request to create a new table.
 * 
 * @author Balraja Subbiah
 */
public class CreateTableResponseMessage extends Message
{
    private String myTableName;
    
    private boolean myIsSuccessful;
    
    private String myErrorMessage;

    /**
     * CTOR
     */
    public CreateTableResponseMessage()
    {
        this(null, null, false, null);
    }

    /**
     * CTOR
     */
    public CreateTableResponseMessage(NodeID  nodeId,
                                      String  tableName,
                                      boolean isSuccessful,
                                      String  errorMessage)
    {
        super(nodeId);
        myTableName = tableName;
        myIsSuccessful = isSuccessful;
        myErrorMessage = errorMessage;
    }

    /**
     * Returns the value of tableName
     */
    public String getTableName()
    {
        return myTableName;
    }

    /**
     * Setter for the tableName
     */
    public void setTableName(String tableName)
    {
        myTableName = tableName;
    }

    /**
     * Returns the value of isSuccessful
     */
    public boolean isIsSuccessful()
    {
        return myIsSuccessful;
    }

    /**
     * Setter for the isSuccessful
     */
    public void setIsSuccessful(boolean isSuccessful)
    {
        myIsSuccessful = isSuccessful;
    }

    /**
     * Returns the value of errorMessage
     */
    public String getErrorMessage()
    {
        return myErrorMessage;
    }

    /**
     * Setter for the errorMessage
     */
    public void setErrorMessage(String errorMessage)
    {
        myErrorMessage = errorMessage;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeUTF(myTableName);
        out.writeBoolean(myIsSuccessful);
        if (myErrorMessage != null) {
            out.writeBoolean(true);
            out.writeUTF(myErrorMessage);
        }
        else {
            out.writeBoolean(false);
        }
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
        myIsSuccessful = in.readBoolean();
        boolean hasError = in.readBoolean();
        if (hasError) {
            myErrorMessage = in.readUTF();
        }
    }
}
