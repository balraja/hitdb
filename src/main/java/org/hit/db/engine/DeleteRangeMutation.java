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
package org.hit.db.engine;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

import org.hit.db.model.Database;
import org.hit.db.model.Mutation;
import org.hit.util.Range;

/**
 * A helper method to delete the specified range on the given table
 * 
 * @author Balraja Subbiah
 */
public class DeleteRangeMutation implements Mutation
{
    private String myTableName;
    
    private Range<?> myDeletedRange;
    
    private List<? extends Object> myData;

    /**
     * CTOR
     */
    public DeleteRangeMutation()
    {
        this(null, null);
    }
    
    
    /**
     * CTOR
     */
    public DeleteRangeMutation(String tableName, Range<?> deletedRange)
    {
        super();
        myTableName = tableName;
        myDeletedRange = deletedRange;
    }
    
    /**
     * Returns the value of deleted data
     */
    public List<? extends Object> getDeletedData()
    {
        return myData;
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
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myTableName);
        out.writeObject(myDeletedRange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
         throws IOException, ClassNotFoundException
    {
        myTableName = in.readUTF();
        myDeletedRange = (Range<?>) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Database database)
    {
        myData = 
            new ArrayList<>(
                database.lookUpTable(myTableName)
                        .deleteRange(myDeletedRange.getMinValue(),
                                     myDeletedRange.getMaxValue()));

    }

}
