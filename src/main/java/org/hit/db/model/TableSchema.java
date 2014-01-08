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
package org.hit.db.model;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Defines the contract for a type that defines the columns in a table.
 * 
 * @author Balraja Subbiah
 */
public class TableSchema implements Externalizable
{
    private String       myTableName;
    
    private List<String> myTableColumns;
    
    private List<String> myPrimaryKey;
    
    /**
     * CTOR
     */
    public TableSchema()
    {
        this(null, null, null);
    }

    /**
     * CTOR
     */
    public TableSchema(String       tableName, 
                  List<String> tableColumns,
                  List<String> primaryKey)
    {
        super();
        myTableName = tableName;
        myTableColumns = tableColumns;
        myPrimaryKey = primaryKey;
    }
    
    /**
     * Returns the value of tableName
     */
    public String getTableName()
    {
        return myTableName;
    }

    /**
     * Returns the value of tableColumns
     */
    public List<String> getTableColumns()
    {
        return myTableColumns;
    }

    /**
     * Returns the value of primaryKey
     */
    public List<String> getPrimaryKey()
    {
        return myPrimaryKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myTableName);
        out.writeObject(myTableColumns);
        out.writeObject(myPrimaryKey);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        myTableName = in.readUTF();
        myTableColumns = (List<String>) in.readObject();
        myPrimaryKey = (List<String>) in.readObject();
    }
}
