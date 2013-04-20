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

package org.hit.db.query.operators;

import java.util.Set;
import java.util.SortedSet;

import org.hit.db.model.Queryable;

/**
 * Defines the contract for key that can be used in grouping operations
 * 
 * @author Balraja Subbiah
 */
public class GroupKey
{
    private final SortedSet<String> myGroupingColumns;
    
    private final Queryable myWrappedObject;

    /**
     * CTOR
     */
    public GroupKey(SortedSet<String> groupingColumns, Queryable wrappedObject)
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
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + myGroupingColumns.hashCode();
        for (String columnName : myGroupingColumns) {
            Object columnValue = myWrappedObject.getFieldValue(columnName);
            result = prime * result
                     + ((myGroupingColumns == null) ? 0
                                                    : columnValue.hashCode());
        }
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
        GroupKey other = (GroupKey) obj;
        if (!getGroupingColumns().equals(other.getGroupingColumns()))
        {
            return false;
        }
        for (String fieldName : getGroupingColumns()) {
            Object value = getValue(fieldName);
            Object value2 = other.getValue(fieldName);
            if ((value == null && value2 != null)
                || (value != null && value2 == null)
                || (!value.equals(value2))) 
            {
                return false;
            }
        }
        return true;
    }
}
