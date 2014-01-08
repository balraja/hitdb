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
import java.util.List;
import java.util.TreeMap;

import org.hit.db.keyspace.LinearKeyspace;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.HitTableSchema;
import org.hit.db.model.Table;
import org.hit.example.DataLoader;

/**
 * A test table that can be used for verifying query execution.
 * 
 * @author Balraja Subbiah
 */
public class TestTable<K extends Comparable<K>, P extends Persistable<K>> 
    implements Table<K, P>
{
    private final TreeMap<K, P> myData;
    
    private final HitTableSchema mySchema;
    
    /**
     * CTOR
     */
    public TestTable(String             tableName, 
                     Class<? extends P> tableClass,
                     Class<? extends K> keyClass,
                     LinearKeyspace<K>  keySpace,
                     DataLoader<P>      dataLoader)
    {
        myData = new TreeMap<>();
        mySchema = new HitTableSchema(tableName,
                              tableClass,
                              keyClass,
                              keySpace);
        
        List<P> data = dataLoader.loadTestData();
        for (P persistable : data) {
            myData.put(persistable.primaryKey(), persistable);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> findMatching(Predicate predicate)
    {
        List<P> result = new ArrayList<>();
        for (P persistable : myData.values()) {
            if (predicate.isInterested(persistable)) {
                result.add(persistable);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> findMatching(Predicate predicate, K start, K end)
    {
        List<P> result = new ArrayList<>();
        for (P persistable : myData.subMap(start, end).values()) {
            if (predicate.isInterested(persistable)) {
                result.add(persistable);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P getRow(K primarykey)
    {
        return myData.get(primarykey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HitTableSchema getSchema()
    {
        return mySchema;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean update(P old, P updated)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P deleteRow(K primaryKey)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> deleteRange(K primaryKey, K secondaryKey)
    {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> deleteRange(Object primaryKey, Object secondaryKey)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
