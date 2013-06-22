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

package org.hit.transactions.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

import org.hit.db.model.Column;
import org.hit.db.model.Database;
import org.hit.db.model.Mutation;
import org.hit.db.model.PartitioningType;
import org.hit.db.model.Query;
import org.hit.db.model.Schema;
import org.hit.db.model.Table;
import org.hit.db.transactions.DatabaseAdaptor;
import org.hit.db.transactions.impl.TransactableHitDatabase;
import org.hit.example.ClimateData;
import org.hit.example.ClimateDataKey;
import org.hit.example.HitDbTest;
import org.hit.key.LinearKeyspace;
import org.hit.key.domain.ComposedDomain;
import org.hit.key.domain.DiscreteDomain;
import org.hit.key.domain.IntegerDomain;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Defines the testcase for validating the functionality of
 * {@link TransactableHitDatabase}
 *
 * @author Balraja Subbiah
 */
public class TransactionTest
{
    private static class SimpleMutation implements Mutation
    {
        private final ClimateData myAddedData;

        /**
         * CTOR
         */
        public SimpleMutation(ClimateData addedData)
        {
            myAddedData = addedData;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException
        {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void update(Database database)
        {
            Table<ClimateDataKey, ClimateData> climateTable =
                database.lookUpTable(HitDbTest.TABLE_NAME);

            climateTable.update(null, myAddedData);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeExternal(ObjectOutput out) throws IOException
        {

        }
    }

    private static class SimpleQuery implements Query
    {
        private final ClimateDataKey myKey;

        /**
         * CTOR
         */
        public SimpleQuery(ClimateDataKey key)
        {
            myKey = key;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object query(Database database)
        {
            Table<ClimateDataKey, ClimateData> climateTable =
                database.lookUpTable(HitDbTest.TABLE_NAME);
            return climateTable.getRow(myKey);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException
        {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeExternal(ObjectOutput out) throws IOException
        {
        }
    }

    @Test
    public void testTransactableDatabase()
    {
        DatabaseAdaptor database =
            new DatabaseAdaptor(new TransactableHitDatabase(), 1L);
        
        DiscreteDomain<ClimateDataKey> domain = 
            new ComposedDomain<ClimateDataKey>(
               Lists.<DiscreteDomain<?>>newArrayList(
                    new IntegerDomain(2013, 2014),
                    new IntegerDomain(1, 365),
                    new IntegerDomain(1, 200)),
               ClimateDataKey.class);

        Schema schema =
            new Schema(HitDbTest.TABLE_NAME,
                       new ArrayList<Column>(),
                       ClimateData.class,
                       ClimateDataKey.class,
                       PartitioningType.PARTITIONABLE,
                       new LinearKeyspace<>(domain));

        database.createTable(schema);
        ClimateData newClimateDataRow =
            new ClimateData(new ClimateDataKey(2013, 1, 1),
                            48,
                            20,
                            25,
                            50,
                            45);

        Mutation m = new SimpleMutation(newClimateDataRow);
        m.update(database);
        Query q = new SimpleQuery(newClimateDataRow.getKey());
        Object result = q.query(database);
        assertNotNull(result);
        assertEquals(result, newClimateDataRow);
    }
}
