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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
import org.hit.db.model.PartitioningType;
import org.hit.db.model.Schema;
import org.hit.event.MasterDownEvent;
import org.hit.key.HashKeyspace;
import org.hit.key.LinearKeyspace;
import org.hit.key.Partition;
import org.hit.key.HashKeyspace.HashFunctionID;
import org.hit.key.domain.DiscreteDomain;
import org.hit.registry.RegistryService;
import org.hit.util.LogFactory;

import com.google.common.hash.Funnel;
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

    private static final String ZK_HIT_DOMAIN_NODE = "domain";

    private static final String ZK_HIT_HASH_FUNCTION_NODE = "hash";

    private static final String ZK_HIT_HASH_FUNNEL_NODE = "funnel";

    private static final String ZK_HIT_HOSTS_ROOT = "/hit_hosts";
    
    private static final String ZK_HIT_HOSTS_MASTER = "master";

    private static final String ZK_HIT_PARTITION_NODE = "partition";

    private static final String ZK_HIT_SCHEMA_LOCK = "lock";

    private static final String ZK_HIT_TABLES_ROOT = "/hit_tables";

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
     * Adds table schema information to the registry
     */
    public void addSchema(Schema schema)
    {
        String path =
           ZK_HIT_TABLES_ROOT + PATH_SEPARATOR + schema.getTableName();

        try {
            Stat tableStat = myZooKeeper.exists(path, false);
            if (tableStat == null) {

                // Create the table node.
                myZooKeeper.create(path,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.PERSISTENT);

                String partitionPath =
                    path + PATH_SEPARATOR + ZK_HIT_PARTITION_NODE;

                myZooKeeper.create(partitionPath,
                                   schema.getKeyPartitioningType()
                                         .name()
                                         .getBytes(),
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.PERSISTENT);

                if (schema.getKeyPartitioningType()
                        == PartitioningType.HASHABLE)
                {
                    HashKeyspace<?> partitioner =
                        (HashKeyspace<?>) schema.getPartitioner();

                    String hashFunctionPath =
                        path + PATH_SEPARATOR + ZK_HIT_HASH_FUNCTION_NODE;

                    myZooKeeper.create(hashFunctionPath,
                                       partitioner.getHashFunctionID()
                                                  .name()
                                                  .getBytes(),
                                       Ids.OPEN_ACL_UNSAFE,
                                       CreateMode.PERSISTENT);

                    String funnelPath =
                        path + PATH_SEPARATOR + ZK_HIT_HASH_FUNNEL_NODE;

                    myZooKeeper.create(funnelPath,
                                       toBytes(partitioner.getFunnel()),
                                       Ids.OPEN_ACL_UNSAFE,
                                       CreateMode.PERSISTENT);
                }
                else {

                    LinearKeyspace<?> partitoner =
                        (LinearKeyspace<?>) schema.getPartitioner();

                    String keySpacePath =
                        path + PATH_SEPARATOR + ZK_HIT_DOMAIN_NODE;

                    myZooKeeper.create(keySpacePath,
                                       toBytes(partitoner.getDomain()),
                                       Ids.OPEN_ACL_UNSAFE,
                                       CreateMode.PERSISTENT);
                }
            }
            else {
                // In future add watchers to the lock. For now ignore it.
            }
        }
        catch (KeeperException | InterruptedException | IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
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
            
            if (myZooKeeper.exists(ZK_HIT_HOSTS_MASTER, false) == null) {
                LOG.info("Created path "+ ZK_HIT_HOSTS_MASTER 
                         + " in the zookeeper");

                myZooKeeper.create(ZK_HIT_HOSTS_MASTER,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.EPHEMERAL);
                
                String masterNodePath = 
                    ZK_HIT_HOSTS_ROOT 
                    + PATH_SEPARATOR
                    + ZK_HIT_HOSTS_MASTER
                    + PATH_SEPARATOR
                    + nodeID.toString();
                    
                myZooKeeper.create(masterNodePath,
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
    public NodeID lookupMasterNode()
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
     * Creates a root node for the table schemas.
     */
    public boolean checkAndCreateTableRoot()
    {
        if (!isUp()) {
            return false;
        }
        try {
            if (myZooKeeper.exists(ZK_HIT_TABLES_ROOT, false) == null) {

                LOG.info("Created path "
                         + ZK_HIT_TABLES_ROOT
                         + " in the zookeeper");

                myZooKeeper.create(ZK_HIT_TABLES_ROOT,
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
    @SuppressWarnings("unchecked")
    @Override
    public <T extends Comparable<T>> Partition<T>
        getTablePartitioner(String tableName)
    {
        String path =
            ZK_HIT_TABLES_ROOT + PATH_SEPARATOR + tableName;
         String lockPath =
             path + PATH_SEPARATOR + ZK_HIT_SCHEMA_LOCK;

         try {

             Stat tableStat = myZooKeeper.exists(path, false);
             Stat lockStat = myZooKeeper.exists(lockPath, false);
             PartitioningType type = null;
             if (tableStat != null && lockStat == null) {

                 String partitionPath =
                     path + PATH_SEPARATOR + ZK_HIT_PARTITION_NODE;

                 String partitioningTypeName =
                     readStringFromNode(partitionPath);

                 if (partitioningTypeName != null) {
                      type = PartitioningType.valueOf(partitioningTypeName);
                 }

                 if (type == PartitioningType.HASHABLE) {

                     String hashFunctionPath =
                         path + PATH_SEPARATOR + ZK_HIT_HASH_FUNCTION_NODE;
                     String hashFunctionName =
                         readStringFromNode(hashFunctionPath);
                     String funnelPath =
                         path + PATH_SEPARATOR + ZK_HIT_HASH_FUNNEL_NODE;

                     Funnel<T> funnel =
                         (Funnel<T>) readObjectFromNode(funnelPath);

                     if (funnel != null && hashFunctionName != null) {
                         return new HashKeyspace<>(
                             HashFunctionID.valueOf(hashFunctionName),
                             funnel);
                     }
                 }
                 else {
                     String domainPath =
                         path + PATH_SEPARATOR + ZK_HIT_DOMAIN_NODE;

                     DiscreteDomain<T> domain =
                         (DiscreteDomain<T>) readObjectFromNode(domainPath);

                     if (domain != null) {
                         return new LinearKeyspace<T>(domain);
                     }
                 }
             }
             return null;
         }
         catch (KeeperException
                | InterruptedException
                | IOException
                | ClassNotFoundException e)
         {
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

    private Object readFrom(byte[] data)
        throws ClassNotFoundException, IOException
    {
        ByteArrayInputStream bin = new ByteArrayInputStream(data);
        ObjectInputStream oin = new ObjectInputStream(bin);
        return oin.readObject();
    }

    private Object readObjectFromNode(String path)
        throws KeeperException,
               InterruptedException,
               ClassNotFoundException,
               IOException
    {
        Stat pathStat = myZooKeeper.exists(path, false);
        return pathStat != null ?
            readFrom(myZooKeeper.getData(path, false, pathStat))
            : null;
    }

    private String readStringFromNode(String path)
        throws KeeperException, InterruptedException
    {
        Stat pathStat = myZooKeeper.exists(path, false);
        return pathStat != null ?
            new String(myZooKeeper.getData(path, false, pathStat))
            : null;
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

    private byte[] toBytes(Object value) throws IOException
    {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        ObjectOutputStream oout = new ObjectOutputStream(bout);
        oout.writeObject(value);
        return bout.toByteArray();
    }
}
