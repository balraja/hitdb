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

package org.hit.zookeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.NodeID;
import org.hit.communicator.nio.IPNodeID;
import org.hit.event.MasterDownEvent;
import org.hit.registry.RegistryService;
import org.hit.util.LogFactory;

import com.google.inject.Inject;

/**
 * A simple class that acts as a client when accessing the zoo keeper.
 *
 * @author Balraja Subbiah
 */
public class ZooKeeperClient implements RegistryService
{
    /**
     * Simple watcher service for listening to updates from zookeeper.
     */
    private class LocalWatcher implements Watcher
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void process(WatchedEvent watchedEvent)
        {
            if (watchedEvent.getState() == KeeperState.SyncConnected) {
                myIsReadyFlag.set(true);
            }
        }
    }

    private class MasterWatcher implements Watcher
    {
        private final EventBus myEventBus;

        /**
         * CTOR
         */
        public MasterWatcher(EventBus eventBus)
        {
            myEventBus = eventBus;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void process(WatchedEvent event)
        {
            if (event.getState() == KeeperState.Disconnected) {
                 try {
                    myEventBus.publish(new MasterDownEvent());
                }
                catch (EventBusException e) {
                    LOG.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
    }

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(ZooKeeperClient.class);

    private static final String PATH_SEPARATOR = "/";

    private static final String ZK_HIT_HOSTS_MASTER = "master";

    private static final String ZK_HIT_HOSTS_ROOT = "/hit_hosts";

    private final AtomicBoolean myIsReadyFlag;

    private final ZooKeeper myZooKeeper;

    /**
     * CTOR
     */
    @Inject
    public ZooKeeperClient(ZooKeeperClientConfig config)
    {
        myIsReadyFlag = new AtomicBoolean(false);
        try {
            myZooKeeper = new ZooKeeper(config.getHosts(),
                                        config.getSessionTimeout(),
                                        new LocalWatcher());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a server node under root that captures the liveliness of node.
     * Clients also access these nodes to determine which servers are alive.
     */
    public void addHostNode(NodeID nodeID)
    {
        try {
            String path =
                ZK_HIT_HOSTS_ROOT + PATH_SEPARATOR + nodeID.toString();

            if (myZooKeeper.exists(path, false) == null) {
                LOG.info("Adding node under path " + path + "to zoo keeper");
                myZooKeeper.create(path,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.EPHEMERAL);
            }
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


    /**
     * Creates a root node for all hit servers.
     */
    public boolean checkAndCreateRootNode()
    {
        if (!isUp()) {
            return false;
        }
        try {
            if (myZooKeeper.exists(ZK_HIT_HOSTS_ROOT, false) == null) {

                LOG.info("Created path "
                          + ZK_HIT_HOSTS_ROOT
                          + " in the zookeeper");

                myZooKeeper.create(ZK_HIT_HOSTS_ROOT,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.PERSISTENT);
            }
            return true;
        }
        catch (KeeperException | InterruptedException e) {
           LOG.log(Level.SEVERE, e.getMessage(), e);
           return false;
        }
    }

    /**
     * Creates a master node under hosts and marks the given node
     * as master
     */
    public boolean claimMasterNode(NodeID nodeID)
    {
        if (!isUp()) {
            return false;
        }

        try {

            if (!checkAndCreateRootNode()) {
                return false;
            }

            String masterPath =
                ZK_HIT_HOSTS_ROOT + PATH_SEPARATOR + ZK_HIT_HOSTS_MASTER;

            if (myZooKeeper.exists(masterPath, false) == null) {

                LOG.info("Created path " + masterPath + " in the zookeeper");

                myZooKeeper.create(masterPath,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.PERSISTENT);

                String nodePath =
                    ZK_HIT_HOSTS_ROOT
                    + PATH_SEPARATOR
                    + ZK_HIT_HOSTS_MASTER
                    + PATH_SEPARATOR
                    + nodeID.toString();

                myZooKeeper.create(nodePath,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.EPHEMERAL);
                return true;
            }
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.toString(), e);
        }
        return false;
    }

    /**
     * A helper method to return the node that's master.
     */
    @Override
    public NodeID getMasterNode()
    {
        try {
            String masterNodePath =
                ZK_HIT_HOSTS_ROOT + PATH_SEPARATOR + ZK_HIT_HOSTS_MASTER;

            for (String child : myZooKeeper.getChildren(masterNodePath, false))
            {
                if (child != null) {
                    return IPNodeID.parseString(child);
                }
            }
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeID> getServerNodes()
    {
        try {
            List<NodeID> servers = new ArrayList<>();
            Stat rootStat = myZooKeeper.exists(ZK_HIT_HOSTS_ROOT, false);
            if (rootStat != null) {
                for (String child :
                        myZooKeeper.getChildren(ZK_HIT_HOSTS_ROOT,
                                                false))
                {
                    servers.add(IPNodeID.parseString(child));
                }
            }
            return servers;
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isUp()
    {
        return myIsReadyFlag.get();
    }


    /**
     * Stops the zookeeper client and closes the session with the zookeeper.
     */
    public void stop()
    {
        try {
            myZooKeeper.close();
        }
        catch (InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * A helper method to register for the watcher service.
     */
    public void watchMaster(EventBus eventBus)
    {
        String masterNodePath =
            ZK_HIT_HOSTS_ROOT + PATH_SEPARATOR + ZK_HIT_HOSTS_MASTER;

        try {
            myZooKeeper.exists(masterNodePath,
                               new MasterWatcher(eventBus));
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }

}
