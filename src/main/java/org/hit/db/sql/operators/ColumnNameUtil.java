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

import org.hit.db.model.Row;

/**
 * Defines an util class that can be used for querying column values.
 * 
 * @author Balraja Subbiah
 */
public final class ColumnNameUtil
{
    public static final String ALL_COLUMNS = "ALL_COLUMNS";
    
    public static final String ALL_COLUMNS_SYMBOLIC = "(*)";
    
    public static String[] nestedColumnNames(String columnName)
    {
        return columnName.split("\\.");
    }
    
    public static Object getValue(Row record, String[] columnNames)
    {
        Object result = record;
        for (String nestedColumn : columnNames) {
            result = ((Row) result).getFieldValue(nestedColumn);
        }
        return result;
    }
    
    public static String[] copyColumnName(String[] columnNames)
    {
        String[] newColumnNames = new String[columnNames.length];
        System.arraycopy(columnNames, 0, newColumnNames, 0, columnNames.length);
        return newColumnNames;
    }
    
    private ColumnNameUtil()
    {
    }
}
