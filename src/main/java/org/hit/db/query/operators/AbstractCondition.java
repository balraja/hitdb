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

import org.hit.db.model.Queryable;

/**
 * An abstract class that defines the common operations used across all 
 * <code>Condition</code>s
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractCondition implements Condition
{
    private final String myColumnName;
    
    private final String[] myNestedColumnNames;

    /**
     * CTOR
     */
    public AbstractCondition(String columnName)
    {
        super();
        String[] columnNames = columnName.split("\\.");
        myColumnName = columnNames[0];
        if (columnNames.length > 1) {
            myNestedColumnNames = new String[columnNames.length -1];
            System.arraycopy(
                columnNames, 
                1, 
                myNestedColumnNames, 
                0, 
                myNestedColumnNames.length);
        }
        else {
            myNestedColumnNames = null;
        }
    }
    
    protected Object getValue(Queryable record)
    {
        if (myNestedColumnNames == null) {
            return record.getFieldValue(myColumnName);
        }
        else {
            Object result = record.getFieldValue(myColumnName);
            for (String nestedColumn : myNestedColumnNames) {
                result = ((Queryable) result).getFieldValue(nestedColumn);
            }
            return result;
        }
    }
}
