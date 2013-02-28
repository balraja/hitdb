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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.hit.db.model.Column;
import org.hit.db.model.DBOperation;
import org.hit.db.model.PartitioningType;
import org.hit.db.model.Schema;
import org.hit.facade.FacadeCallback;
import org.hit.facade.HitDBFacade;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;
import org.hit.util.LogFactory;

/**
 * Defines the contract for the client that can be used for testing the
 * database.
 *
 * @author Balraja Subbiah
 */
public class HitDbTest implements Application
{
    private class SimpleFacadeCallback implements FacadeCallback
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void onDBOperationFailure(DBOperation operation,
                                         String message,
                                         Throwable exception)
        {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onDBOperationSuccess(DBOperation operation)
        {

        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onTableCreationFailure(String tableName, String message)
        {
            myIsResponseReceived.compareAndSet(false, true);
            LOG.info("Creation of the table " + tableName + " failed because"
                     + " of " + message);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onTableCreationSuccess(String tableName)
        {
            myIsResponseReceived.compareAndSet(false, true);
            LOG.info("Creation of the table " + tableName + " succedded");
        }
    }

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(HitDbTest.class);

    public static final String TABLE_NAME = "ClimateData";

    /**
     * Main method that launches the application.
     */
    public static void main(String[] args)
    {
        ApplicationLauncher appLauncher =
            new ApplicationLauncher(new HitDbTest());
        appLauncher.launch();
    }

    private final AtomicBoolean myIsResponseReceived;

    private final HitDBFacade myServerFacade;

    /**
     * CTOR
     */
    public HitDbTest()
    {
        myServerFacade = new HitDBFacade();
        myIsResponseReceived = new AtomicBoolean(false);
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
                       new ClimateDataKeySpace());

        myServerFacade.createTable(schema, new SimpleFacadeCallback());
        while (!myIsResponseReceived.get()) {

        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        myServerFacade.start();
        createTable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        myServerFacade.stop();
    }
}
