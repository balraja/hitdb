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
package org.hit.db.sql.operators;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.SortedSet;

import org.hit.db.model.Row;

/**
 * @author Balraja Subbiah
 */
public class GroupingColumnsKey implements GroupKey
{
    private SortedSet<String> myGroupingColumns;
    
    private Row myWrappedObject;
    
    /**
     * CTOR
     */
    public GroupingColumnsKey()
    {
        this(null, null);
    }

    /**
     * CTOR
     */
    public GroupingColumnsKey(
        SortedSet<String> groupingColumns, Row wrappedObject)
    {
        super();
        myGroupingColumns = groupingColumns;
        myWrappedObject = wrappedObject;
    }
    
    /**
     * Returns the value of groupingColumns
     */
    public SortedSet<String> getGroupingColumns()
    {
        return myGroupingColumns;
    }

    /**
     * Returns the value of the specified field.
     */
    public Object getValue(String fieldName)
    {
        return myWrappedObject.getFieldValue(fieldName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myGroupingColumns);
        out.writeObject(myWrappedObject);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        myGroupingColumns = (SortedSet<String>) in.readObject();
        myWrappedObject   = (Row) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((myGroupingColumns == null) ? 0 : myGroupingColumns
                        .hashCode());
        result = prime * result
                + ((myWrappedObject == null) ? 0 : myWrappedObject.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        GroupingColumnsKey other = (GroupingColumnsKey) obj;
        if (myGroupingColumns == null) {
            if (other.myGroupingColumns != null)
                return false;
        }
        else if (!myGroupingColumns.equals(other.myGroupingColumns))
            return false;
        if (myWrappedObject == null) {
            if (other.myWrappedObject != null)
                return false;
        }
        else if (!myWrappedObject.equals(other.myWrappedObject))
            return false;
        return true;
    }
}
