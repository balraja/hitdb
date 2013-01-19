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

package org.hit.client;

import org.hit.communicator.Communicator;
import org.hit.communicator.NodeID;
import org.hit.db.model.Schema;
import org.hit.di.HitClientModule;
import org.hit.event.CreateTableMessage;
import org.hit.registry.RegistryService;
import org.hit.topology.Topology;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;

import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Defines the contract for a class that acts as a client to the database.
 * Primarily this class reads the commands/statements from the command line
 * and after parsing the same sends it down to the servers.
 * 
 * @author Balraja Subbiah
 */
public class HitClient implements Application
{
    /**
     * The main method that launches the client.
     */
    public static void main(String[] args)
    {
        ApplicationLauncher launcher = new ApplicationLauncher(new HitClient());
        launcher.launch();
    }
    
    private final Communicator myCommunicator;
    
    private final RegistryService myRegistryService;
    
    private final NodeID myClientID;

    /**
     * CTOR
     */
    public HitClient()
    {
        Injector injector = Guice.createInjector(new HitClientModule());
        myCommunicator = injector.getInstance(Communicator.class);
        myRegistryService = injector.getInstance(RegistryService.class);
        myClientID = injector.getInstance(NodeID.class);
    }

    public void createTable(Schema schema)
    {
        Topology topology = myRegistryService.getTopology();
        CreateTableMessage message =
            new CreateTableMessage(myClientID, schema);
        
        for (NodeID nodeID : topology.getNodes()) {
            myCommunicator.sendTo(nodeID, message);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
    }
}
