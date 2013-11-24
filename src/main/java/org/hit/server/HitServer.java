/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

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

package org.hit.server;

import java.util.logging.Logger;

import org.hit.di.HitServerModule;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;
import org.hit.util.LogFactory;
import org.hit.zookeeper.ZooKeeperClient;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * The main application that starts the database engine.
 *
 * @author Balraja Subbiah
 */
public class HitServer implements Application
{
    private static final Logger LOG = LogFactory.getInstance()
                                                .getLogger(HitServer.class);

    /** The main method that launches the system */
    public static void main(String[] args)
    {
        ApplicationLauncher launcher =
            new ApplicationLauncher(new HitServer());
        launcher.launch();
    }

    private final ServerComponentManager myServerComponentManager;
    
    private final ZooKeeperClient   myZooKeeperClient;
    
    /**
     * CTOR
     */
    public HitServer()
    {
        Injector injector = Guice.createInjector(new HitServerModule());
        myServerComponentManager = 
            injector.getInstance(ServerComponentManager.class);
        myZooKeeperClient = 
            injector.getInstance(ZooKeeperClient.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        LOG.info("Hit db server starting up");
        while (!myZooKeeperClient.isUp()) {
            //Wait till zookeeper client becomes ready.
        }
        LOG.info("Connected to zookeeper");
        myServerComponentManager.start();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
       myServerComponentManager.stop();
    }
}
