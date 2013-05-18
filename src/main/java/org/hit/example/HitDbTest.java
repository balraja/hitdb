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
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.client.DBClient;
import org.hit.db.model.Column;
import org.hit.db.model.PartitioningType;
import org.hit.db.model.Schema;
import org.hit.facade.DBOperationResponse;
import org.hit.facade.HitDBFacade;
import org.hit.facade.TableCreationResponse;
import org.hit.partitioner.LinearPartitioner;
import org.hit.util.LogFactory;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Defines the contract for the client that can be used for testing the
 * database.
 *
 * @author Balraja Subbiah
 */
public class HitDbTest implements DBClient
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HitDbTest.class);

    public static final String TABLE_NAME = "ClimateData";

    private HitDBFacade myServerFacade;

    /**
     * CTOR
     */
    public HitDbTest()
    {
        myServerFacade = new HitDBFacade();
    }

    /** Creates the climate data table */
    public void createTable()
    {
        Schema schema =
            new Schema(TABLE_NAME,
                       new ArrayList<Column>(),
                       ClimateData.class,
                       ClimateDataKey.class,
                       PartitioningType.PARTITIONABLE,
                       new LinearPartitioner<>(new ClimateDataKeySpace()));

        ListenableFuture<TableCreationResponse> futureResponse =
            myServerFacade.createTable(schema);

        try {
            TableCreationResponse result = futureResponse.get();
            LOG.info("Creation of table "
                     + result.getTableSchema().getTableName()
                     + " succedded on nodes " + result.getAffectedNodes());
        }
        catch (InterruptedException e) {
            // ignore
        }
        catch (ExecutionException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(HitDBFacade facade)
    {
        myServerFacade = facade;
        createTable();
        updateTable();
    }

    /** Updates the table */
    public void updateTable()
    {
        AddRowMutation mutation =
            new AddRowMutation(
               new ClimateData(new ClimateDataKey(2013, 1, 1),
                               48,
                               20,
                               25,
                               50,
                               45));
        ListenableFuture<DBOperationResponse> futureResponse =
            myServerFacade.apply(mutation, TABLE_NAME);

        try {
            DBOperationResponse response = futureResponse.get();
            LOG.info("Updation of "
                     + TABLE_NAME
                     + " with mutation "
                     + response.getDbOperation().getClass().getSimpleName()
                     + " succedded");
        }
        catch (InterruptedException e) {
        }
        catch (ExecutionException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
