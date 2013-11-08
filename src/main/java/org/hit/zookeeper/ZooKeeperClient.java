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

import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
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
import org.hit.communicator.NodeID;
import org.hit.communicator.nio.IPNodeID;
import org.hit.event.MasterDownEvent;
import org.hit.util.LogFactory;
import org.hit.util.Pair;

import com.google.inject.Inject;

/**
 * A simple class that acts as a client when accessing the zoo keeper.
 *
 * @author Balraja Subbiah
 */
public class ZooKeeperClient
{
    public static final String PATH_SEPARATOR = "/";
    
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(ZooKeeperClient.class);
    
    private static final String LOCK_NODE = "lock";

    private final AtomicBoolean myIsReadyFlag;

    private final ZooKeeper myZooKeeper;
    
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
    
    private static class SeqNodeComparator 
        implements Comparator<Pair<String,Long>> 
    {

        /**
         * {@inheritDoc}
         */
        @Override
        public
            int compare(Pair<String, Long> o1, Pair<String, Long> o2)
        {
            return o1.getSecond().intValue() - o2.getSecond().intValue();
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
     * Creates a given path under root.
     */
    public boolean createPersistentPath(String path)
    {
        if (!isUp()) {
            return false;
        }
        try {
            if (myZooKeeper.exists(path, false) == null) {
                LOG.info("Adding node under path " + path + "to zoo keeper");
                myZooKeeper.create(path,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.PERSISTENT);
            }
            return true;
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Adds a node under the given path
     */
    public void addNode(String treePath, NodeID nodeID)
    {
        try {
            String path =
                treePath + PATH_SEPARATOR + nodeID.toString();

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
     *  A helper method to add {@link Watcher} to the zkNode at 
     *  the specified path.
     */
    public boolean addWatcher(String path, Watcher watcher)
    {
        try {
            myZooKeeper.exists(path, watcher);
            return true;
        }
        catch (KeeperException | InterruptedException e) {
            return false;
        }
    }
    
    /**
     *  A helper method to add {@link Watcher} to the zkNode at 
     *  the specified path.
     */
    public boolean addWatcherToLockHolder(String path, Watcher watcher)
    {
        try {
            myZooKeeper.exists(path, watcher);
            return true;
        }
        catch (KeeperException | InterruptedException e) {
            return false;
        }
    }
    
    /**
     * Returns true if the LOCK node is available under the specified path
     */
    public boolean isLockNodeAvailable(String treePath)
    {
        String lockPath = treePath + PATH_SEPARATOR + LOCK_NODE;
        try {
            return (myZooKeeper.exists(lockPath, true) != null);
        }
        catch (KeeperException | InterruptedException e) {
            return false;
        }
    }
    
    /**
     * A helper method to acquire lock under a given path using 
     * zookeeper's sequential nodes.
     */
    public boolean acquireLockUnder(String treePath, NodeID nodeID)
    {
        try {
            String path =
                treePath + PATH_SEPARATOR + LOCK_NODE;
            createPersistentPath(path);
            String createdPath = null;
            Stat stat = new Stat();
            
            for (String child : myZooKeeper.getChildren(path, false)) {
                String childPath = path + PATH_SEPARATOR + child;
                byte[] data = myZooKeeper.getData(childPath, null, stat);
                NodeID childId = IPNodeID.parseString(new String(data));
                if (childId.equals(nodeID)) {
                    createdPath = child;
                }
            }
            
            if (createdPath == null) {
                createdPath = 
                    myZooKeeper.create(
                        path,
                        nodeID.toString().getBytes(),
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
            }
            
            Long id = Long.valueOf(createdPath);
            SortedSet<Pair<String,Long>> sequenceNumbers = 
                new TreeSet<>(new SeqNodeComparator());
            for (String child : myZooKeeper.getChildren(path, false)) {
                sequenceNumbers.add(new Pair<>(child, Long.valueOf(child)));
            }
            return (sequenceNumbers.first().getSecond() == id);
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Returns the sequence number under which lock is hold for a given path.
     */
    public long getLockSequenceNO(String path)
    {
        String lockPath = path + PATH_SEPARATOR + LOCK_NODE;
     
        try {
            TLongSet sequenceNumbers = new TLongHashSet();
            for (String child : myZooKeeper.getChildren(lockPath, false)) {
                sequenceNumbers.add(Long.parseLong(child));
            }
            long[] sequenceArray = sequenceNumbers.toArray();
            Arrays.sort(sequenceArray);
            return sequenceArray[0];
        }
        catch (NumberFormatException | KeeperException | InterruptedException e) 
        {
            return -1L;
        }
    }

    /**
     * A helper method to return the node that holds a lock under a 
     * path.
     */
    public NodeID getLockHolder(String path)
    {
        try {
            String lockPath = path + PATH_SEPARATOR + LOCK_NODE;
            SortedSet<Pair<String,Long>> sequenceNumbers = 
                new TreeSet<>(new SeqNodeComparator());
            for (String child : myZooKeeper.getChildren(lockPath, false))
            {
                sequenceNumbers.add(new Pair<>(child, Long.valueOf(child)));
            }
            String childPath = 
                path + PATH_SEPARATOR + sequenceNumbers.first().getFirst();
            byte[] data = myZooKeeper.getData(childPath, null, new Stat());
            return IPNodeID.parseString(new String(data));
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return null;
    }

    /**
     * Returns a list of {@link NodeID}s under a path
     */
    public List<NodeID> getNodes(String path)
    {
        try {
            List<NodeID> servers = new ArrayList<>();
            Stat rootStat = myZooKeeper.exists(path, false);
            if (rootStat != null) {
                for (String child :
                        myZooKeeper.getChildren(path, false))
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
     * Returns true if we have connected successfully with the zookeeper.
     */
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
}
