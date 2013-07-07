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

package org.hit.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.client.DBClient;
import org.hit.db.model.Column;
import org.hit.db.model.Schema;
import org.hit.db.model.mutations.BatchMutation;
import org.hit.facade.DBOperationResponse;
import org.hit.facade.TableCreationResponse;
import org.hit.key.LinearKeyspace;
import org.hit.key.domain.LongDomain;
import org.hit.util.LogFactory;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Defines the contract for the client that can be used for testing the
 * database.
 *
 * @author Balraja Subbiah
 */
public class HitDbTest extends DBClient
{
    private static final String AIRPORT_FILE = "airports.txt";

    private static final String COMMENT = "/**";

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HitDbTest.class);

    public static final String TABLE_NAME = "airports";

    /**
     * CTOR
     */
    public HitDbTest()
    {
    }

    /** Creates the climate data table */
    public void createTable()
    {
        Schema schema =
            new Schema(TABLE_NAME,
                       new ArrayList<Column>(),
                       Airport.class,
                       Long.class,
                       new LinearKeyspace<Long>(new LongDomain(1L, 7000L)));

        ListenableFuture<TableCreationResponse> futureResponse =
            getFacade().createTable(schema);

        try {
            TableCreationResponse result = futureResponse.get();
            LOG.info("Creation of table " + result.getTableName());
        }
        catch (InterruptedException e) {
            // ignore
        }
        catch (ExecutionException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * A helper method to read the data from test file
     */
    public List<Airport> readTestData()
    {
        String line = null;
        List<Airport> airportData = new ArrayList<>();
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(
                     HitDbTest.class.getClassLoader()
                                    .getResourceAsStream(AIRPORT_FILE))))
        {
            while ((line = reader.readLine()) != null) {

                if (line.startsWith(COMMENT)) {
                    continue;
                }

                String[] parts = line.split(",");
                airportData.add(new Airport(
                    Long.parseLong(parts[0]),
                    parts[1],
                    parts[2],
                    parts[3],
                    parts[4],
                    Double.parseDouble(parts[6]),
                    Double.parseDouble(parts[7]),
                    Double.parseDouble(parts[8]),
                    Float.parseFloat(parts[9])));

            }
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return airportData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        super.start();
        createTable();
        updateTable();
    }

    /** Updates the table */
    public void updateTable()
    {
        BatchMutation<Long, Airport> mutation = new BatchMutation<>(
                TABLE_NAME, readTestData());

        ListenableFuture<DBOperationResponse> futureResponse =
            getFacade().apply(mutation);

        try {
            futureResponse.get();
            LOG.info("Updation of "
                     + TABLE_NAME
                     + " with mutation "
                     + mutation.getClass().getSimpleName()
                     + " succedded");
        }
        catch (InterruptedException e) {
        }
        catch (ExecutionException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
