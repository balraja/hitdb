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

import org.hit.db.model.Queryable;

/**
 * Defines the contract for <code>Queryable</code> which stores the 
 * objects from different tables indexed by their table name.
 * 
 * @author Balraja Subbiah
 */
public class TableIndexedQueryable implements Queryable
{
    private final Map<String, Queryable> myTable2ObjectIndex;
    
    /**
     * CTOR
     */
    public TableIndexedQueryable(Map<String, Queryable> table2ObjectIndex)
    {
        myTable2ObjectIndex = table2ObjectIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFieldValue(String fieldName)
    {
        return myTable2ObjectIndex.get(fieldName);
    }
}
