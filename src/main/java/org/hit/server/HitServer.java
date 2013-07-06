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

import org.hit.actors.Actor;
import org.hit.communicator.CommunicatingActor;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusManager;
import org.hit.db.engine.DBEngine;
import org.hit.db.engine.EngineConfig;
import org.hit.db.engine.LocalWarden;
import org.hit.db.engine.MasterWarden;
import org.hit.di.HitServerModule;
import org.hit.gossip.Disseminator;
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

    private final CommunicatingActor myCommunicatingActor;

    private final EngineConfig         myConfig;

    private final ConsensusManager   myConsensusManager;

    private final DBEngine           myDBEngine;

    private final Disseminator       myDisseminator;

    private final NodeID             myServerNodeID;

    private final ZooKeeperClient    myZooKeeperClient;

    /**
     * CTOR
     */
    public HitServer()
    {
        Injector injector = Guice.createInjector(new HitServerModule());
        myServerNodeID = injector.getInstance(NodeID.class);
        myCommunicatingActor = injector.getInstance(CommunicatingActor.class);
        myConsensusManager = injector.getInstance(ConsensusManager.class);
        myDisseminator = injector.getInstance(Disseminator.class);
        myDBEngine      = injector.getInstance(DBEngine.class);
        myZooKeeperClient = injector.getInstance(ZooKeeperClient.class);
        myConfig = injector.getInstance(EngineConfig.class);
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
        ;

        myZooKeeperClient.checkAndCreateRootNode();
        myZooKeeperClient.addHostNode(myServerNodeID);
        LOG.info("Node registered with the zookeeper");

        if (myConfig.isMaster()) {
            while(!myZooKeeperClient.claimMasterNode(myServerNodeID)) {
            }
            LOG.info("Master node claimed in zookeeper");
        }
        else {
           NodeID masterNode = myZooKeeperClient.getMasterNode();
           while (masterNode == null) {
               // Spin till the master is started
               masterNode = myZooKeeperClient.getMasterNode();
           }
           myDBEngine.init(masterNode);
        }
        
        myCommunicatingActor.start();
        LOG.info("Communicator started");
        myConsensusManager.start();
        LOG.info("Consensus manager started");
        myDisseminator.start();
        LOG.info("Gossiper started");
        myDBEngine.start();
        LOG.info("Database engine started");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        myCommunicatingActor.stop();
        myConsensusManager.stop();
        myDisseminator.stop();
        myDBEngine.stop();
        myDisseminator.stop();
    }
}
