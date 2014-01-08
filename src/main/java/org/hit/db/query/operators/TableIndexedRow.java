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

import java.util.Map;

import org.hit.db.model.Row;

/**
 * Defines the contract for <code>Row</code> which stores the 
 * objects from different tables indexed by their table name.
 * 
 * @author Balraja Subbiah
 */
public class TableIndexedRow implements Row
{
    private final Map<String, Row> myTable2ObjectIndex;
    
    /**
     * CTOR
     */
    public TableIndexedRow(Map<String, Row> table2ObjectIndex)
    {
        myTable2ObjectIndex = table2ObjectIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFieldValue(String tableName)
    {
        return myTable2ObjectIndex.get(tableName);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return myTable2ObjectIndex.toString();
    }
}
