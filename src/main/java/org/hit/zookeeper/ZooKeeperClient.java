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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.hit.communicator.NodeID;
import org.hit.communicator.nio.IPNodeID;
import org.hit.util.LogFactory;

import com.google.inject.Inject;

/**
 * A simple class that acts as a client when accessing the zoo keeper.
 * 
 * @author Balraja Subbiah
 */
public class ZooKeeperClient
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(ZooKeeperClient.class);
    
    private static final String ZOOKEEPER_HIT_HOSTS_ROOT = "/hit_hosts";
    
    private static final String PATH_SEPARATOR = "/";
    
    private final ZooKeeper myZooKeeper;
    
    private final AtomicBoolean myIsReadyFlag;
    
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
     * Returns true if the client is ready for communicating with the zookeeper 
     */
    public boolean isReady()
    {
        return myIsReadyFlag.get();
    }
    
    /** 
     * Creates a root node for all hit servers.
     */
    public void check_and_create_root_node()
    {
        if (!isReady()) {
            return;
        }
        try {
            if (myZooKeeper.exists(ZOOKEEPER_HIT_HOSTS_ROOT, false) == null) {
                myZooKeeper.create(ZOOKEEPER_HIT_HOSTS_ROOT,
                                   null, 
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.PERSISTENT);
            }
        }
        catch (KeeperException | InterruptedException e) {
           LOG.log(Level.SEVERE, e.getMessage(), e);
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
            IPNodeID ipNodeID = (IPNodeID) nodeID;
            
            String path = 
                ZOOKEEPER_HIT_HOSTS_ROOT 
                + PATH_SEPARATOR 
                + ipNodeID.getIPAddress().toString();
            
            if (myZooKeeper.exists(path, false) == null) {
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
     * Returns the list of <code>NodeID</code>'s of the servers.
     */
    public List<NodeID> getServerIDS()
    {
        try {
            List<NodeID> servers = new ArrayList<>();
            Stat rootStat = myZooKeeper.exists(ZOOKEEPER_HIT_HOSTS_ROOT, false);
            if (rootStat != null) {
                for (String child : 
                        myZooKeeper.getChildren(ZOOKEEPER_HIT_HOSTS_ROOT,
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
}
