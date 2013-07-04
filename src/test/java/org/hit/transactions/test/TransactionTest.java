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
import org.hit.db.model.Query;
import org.hit.db.model.Schema;
import org.hit.db.model.Table;
import org.hit.db.transactions.DatabaseAdaptor;
import org.hit.db.transactions.impl.TransactableHitDatabase;
import org.hit.example.Airport;
import org.hit.example.HitDbTest;
import org.hit.key.LinearKeyspace;
import org.hit.key.domain.LongDomain;
import org.junit.Test;

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
        private final Airport myAddedData;

        /**
         * CTOR
         */
        public SimpleMutation(Airport addedData)
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
            Table<Long, Airport> airportTable =
                database.lookUpTable(HitDbTest.TABLE_NAME);

            airportTable.update(null, myAddedData);
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
        private final Long myKey;

        /**
         * CTOR
         */
        public SimpleQuery(Long key)
        {
            myKey = key;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Object query(Database database)
        {
            Table<Long,Airport> airportTable =
                database.lookUpTable(HitDbTest.TABLE_NAME);
            return airportTable.getRow(myKey);
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

        Schema schema =
            new Schema(HitDbTest.TABLE_NAME,
                       new ArrayList<Column>(),
                       Airport.class,
                       Long.class,
                       new LinearKeyspace<>(new LongDomain(1, 7000)));

        database.createTable(schema);
        Airport airport  =
            new Airport(1,
                        "Heathrow",
                        "London",
                        "United Kingdom",
                        "LHR",
                        51.4775D,
                        -0.461389D,
                        83.0D,
                        0);

        Mutation m = new SimpleMutation(airport);
        m.update(database);
        Query q = new SimpleQuery(airport.primaryKey());
        Object result = q.query(database);
        assertNotNull(result);
        assertEquals(result, airport);
    }
}
