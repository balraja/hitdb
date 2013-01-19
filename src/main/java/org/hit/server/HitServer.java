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

import org.hit.communicator.CommunicatingActor;
import org.hit.consensus.ConsensusManager;
import org.hit.di.HitServerModule;
import org.hit.hms.HealthMonitor;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * An instance of HIT database engine.
 * 
 * @author Balraja Subbiah
 */
public class HitServer implements Application
{
    /** The main method that launches the system */
    public static void main(String[] args)
    {
        ApplicationLauncher launcher =
            new ApplicationLauncher(new HitServer());
        launcher.launch();
    }
    
    private final CommunicatingActor myCommunicatingActor;
    
    private final ConsensusManager   myConsensusManager;
    
    private final HealthMonitor      myHealthMonitor;

    /**
     * CTOR
     */
    public HitServer()
    {
        Injector injector = Guice.createInjector(new HitServerModule());
        myCommunicatingActor = injector.getInstance(CommunicatingActor.class);
        myConsensusManager = injector.getInstance(ConsensusManager.class);
        myHealthMonitor = injector.getInstance(HealthMonitor.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        myCommunicatingActor.start();
        myConsensusManager.start();
        myHealthMonitor.start();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        myCommunicatingActor.stop();
        myConsensusManager.stop();
        myHealthMonitor.stop();
    }
}
