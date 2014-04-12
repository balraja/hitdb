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

import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectLongHashMap;

import org.hit.pool.PoolConfiguration;
import org.hit.pool.Poolable;

/**
 * Defines the contract for an <code>Event</code> that publishes the 
 * db statistics to other parts of the system. For now it prints the
 * number of rows in each table.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(initialSize = 100, size = 1000)
public class DBStatEvent implements Event, Poolable
{
    private final TObjectLongMap<String> myTableToRowCountMap;

    /**
     * CTOR
     */
    public DBStatEvent()
    {
        super();
        myTableToRowCountMap = new TObjectLongHashMap<>();
    }
    
    public void addTableRowCount(String tableName, long rowCount)
    {
        myTableToRowCountMap.put(tableName, rowCount);
    }

    /**
     * Returns the value of tableToRowCountMap
     */
    public TObjectLongMap<String> getTableToRowCountMap()
    {
        return myTableToRowCountMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myTableToRowCountMap.clear();
    }

}
