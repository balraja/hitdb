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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.client.DBClient;
import org.hit.db.keyspace.LinearKeyspace;
import org.hit.db.keyspace.domain.LongDomain;
import org.hit.db.model.Column;
import org.hit.db.model.HitTableSchema;
import org.hit.db.model.mutations.BatchAddMutation;
import org.hit.facade.DBOperationResponse;
import org.hit.facade.TableCreationResponse;
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
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HitDbTest.class);

    public static final String TABLE_NAME = "airports";
    
    private final List<Airport> myTestData;

    /**
     * CTOR
     */
    public HitDbTest()
    {
        myTestData = new LinkedList<>();
    }

    /** Creates the climate data table */
    public void createTable()
    {
        HitTableSchema schema =
            new HitTableSchema(TABLE_NAME,
                       new ArrayList<Column>(),
                       Airport.class,
                       Long.class,
                       new LinearKeyspace<Long>(new LongDomain(1L, 7000L)));

        ListenableFuture<TableCreationResponse> futureResponse =
            getFacade().createTable(schema);

        try {
            TableCreationResponse result = futureResponse.get();
            LOG.info("Creation of table " + result.getTableName()
                     + " is successful ");
            myTestData.addAll(new AirportDataLoader().loadTestData());
            while (!myTestData.isEmpty()) {
                updateTable();
            }
            LOG.info("Successfully added all records corresponding to " 
                     + TABLE_NAME);
        }
        catch (InterruptedException e) {
            // ignore
        }
        catch (ExecutionException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        shutdown();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        super.start();
        createTable();
    }

    /** Updates the table */
    public void updateTable()
    {
        if (myTestData.isEmpty()) {
            return;
        }
        List<Airport> batchedData = new ArrayList<>();
        if (myTestData.size() > 10) {
            for (int i = 0; i < 10; i++) {
                batchedData.add(myTestData.remove(0));
            }
        }
        else {
            batchedData.addAll(myTestData);
            myTestData.clear();
        }
        BatchAddMutation<Long, Airport> mutation = new BatchAddMutation<>(
                TABLE_NAME, batchedData);
        
        LOG.info("Updating table with key range " + mutation.getKeyRange());

        ListenableFuture<DBOperationResponse> futureResponse =
            getFacade().apply(mutation);

        try {
            DBOperationResponse response = futureResponse.get();
            if (response != null) {
                LOG.info("Updation of "
                         + TABLE_NAME
                         + " with mutation "
                         + mutation.getClass().getSimpleName()
                         + " succedded");
            }
        }
        catch (InterruptedException e) {
        }
        catch (ExecutionException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
