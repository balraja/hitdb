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
import org.hit.example.Airline;
import org.hit.example.AirlineDataLoader;
import org.hit.example.Airport;
import org.hit.example.AirportDataLoader;
import org.hit.example.HitDbTest;
import org.hit.example.Route;
import org.hit.example.RouteDataLoader;
import org.hit.key.LinearKeyspace;
import org.hit.key.domain.LongDomain;

/**
 * Extends <code>Database</code> to support loading table data from the 
 * files.
 * 
 * @author Balraja Subbiah
 */
public class TestDB implements Database
{
    private final Map<String, Table<?, ?>> myNameTableMap;
    
    /**
     * CTOR
     */
    public TestDB()
    {
        myNameTableMap = new HashMap<String, Table<?,?>>();
        myNameTableMap.put(HitDbTest.TABLE_NAME,
                           new TestTable<>(HitDbTest.TABLE_NAME, 
                                           Airport.class, 
                                           Long.class,
                                           new LinearKeyspace<>(
                                               new LongDomain(1, 7000)), 
                                           new AirportDataLoader()));
        myNameTableMap.put(Airline.TABLE_NAME,
                           new TestTable<>(Airline.TABLE_NAME, 
                                           Airline.class, 
                                           Long.class,
                                           new LinearKeyspace<>(
                                               new LongDomain(1, 7000)), 
                                           new AirlineDataLoader()));
        myNameTableMap.put(Route.TABLE_NAME,
                           new TestTable<>(Route.TABLE_NAME, 
                                           Route.class, 
                                           Long.class,
                                           new LinearKeyspace<>(
                                               new LongDomain(1, 7000)), 
                                           new RouteDataLoader()));
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
