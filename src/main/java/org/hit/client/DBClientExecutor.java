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

package org.hit.client;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.facade.HitDBFacade;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;
import org.hit.util.LogFactory;

/**
 * Defines the class that executes the client code
 *
 * @author Balraja Subbiah
 */
public class DBClientExecutor implements Application
{
    public static final String CLIENT_CLASS_NAME_PROPERTY =
        "org.hit.client.className";

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(DBClientExecutor.class);

    /**
     * Main method that launches the application.
     */
    public static void main(String[] args)
    {
        ApplicationLauncher appLauncher =
            new ApplicationLauncher(new DBClientExecutor());
        appLauncher.launch();
    }

    private final DBClient myClient;

    private final HitDBFacade myServerFacade;

    /**
     * CTOR
     */
    public DBClientExecutor()
    {
        myServerFacade = new HitDBFacade();
        String className = System.getProperty(CLIENT_CLASS_NAME_PROPERTY);
        if (className == null) {
            throw new RuntimeException("The client class is not available");
        }
        try {
            Class<?> clientClass = Class.forName(className);
            myClient = (DBClient) clientClass.newInstance();
        }
        catch (ClassNotFoundException
               | InstantiationException
               | IllegalAccessException e)
        {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        myServerFacade.start();
        myClient.start(myServerFacade);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        myClient.shutdown();
        myServerFacade.stop();
    }
}
