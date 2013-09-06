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

package org.hit.query.test;

import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Database;
import org.hit.db.model.Persistable;
import org.hit.db.model.Schema;
import org.hit.db.model.Table;
import org.hit.example.HitDbTest;

/**
 * @author Balraja Subbiah
 */
public class TestDB implements Database
{
    private final Map<String, Table<?, ?>> myNameTableMap;
    
    public TestDB()
    {
        myNameTableMap = new HashMap<String, Table<?,?>>();
        myNameTableMap.put(HitDbTest.TABLE_NAME, new AirportTable());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void createTable(Schema schema)
    {
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <K extends Comparable<K>, P extends Persistable<K>> Table<K, P>
        lookUpTable(String tableName)
    {
        return (Table<K, P>) myNameTableMap.get(tableName);
    }
}
