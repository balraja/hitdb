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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.hit.db.model.Column;
import org.hit.db.model.Predicate;
import org.hit.db.model.Schema;
import org.hit.db.model.Table;
import org.hit.example.Airport;
import org.hit.example.AirportDataLoader;
import org.hit.example.HitDbTest;
import org.hit.key.LinearKeyspace;
import org.hit.key.domain.LongDomain;

/**
 * A test table that can be used for verifying query execution.
 * 
 * @author Balraja Subbiah
 */
public class AirportTable implements Table<Long,Airport>
{
    private final TreeMap<Long, Airport> myData;
    
    private final Schema mySchema;
    
    /**
     * CTOR
     */
    public AirportTable()
    {
        myData = new TreeMap<>();
        mySchema = new Schema(HitDbTest.TABLE_NAME,
                              new ArrayList<Column>(),
                              Airport.class,
                              Long.class,
                              new LinearKeyspace<Long>(new LongDomain(1L, 
                                                                      7000L)));
        List<Airport> airports = AirportDataLoader.loadTestData();
        for (Airport airport : airports) {
            myData.put(airport.primaryKey(), airport);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Airport> findMatching(Predicate predicate)
    {
        List<Airport> result = new ArrayList<>();
        for (Airport airport : myData.values()) {
            if (predicate.isInterested(airport)) {
                result.add(airport);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Airport> findMatching(Predicate predicate,
                                            Long start,
                                            Long end)
    {
        List<Airport> result = new ArrayList<>();
        for (Airport airport : myData.subMap(start, end).values()) {
            if (predicate.isInterested(airport)) {
                result.add(airport);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Airport getRow(Long primarykey)
    {
        return myData.get(primarykey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema getSchema()
    {
        return mySchema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean update(Airport old, Airport updated)
    {
        return false;
    }
}
