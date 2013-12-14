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
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.ZooTrace;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.communicator.nio.IPNodeID;
import org.hit.event.MasterDownEvent;
import org.hit.server.ServerNodeID;
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
    
    private static final String PARTICIPANTS_NODE = "participants";

    private final ZooKeeper myZooKeeper;
    
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
        try {
            myZooKeeper = new ZooKeeper(config.getHosts(),
                                        config.getSessionTimeout(),
                                        new Watcher() {

                                            @Override
                                            public void process(
                                                    WatchedEvent arg0)
                                            {
                                            }
                
                                        });
            ZooTrace.setTextTraceLevel(ZooTrace.CLIENT_REQUEST_TRACE_MASK);
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
            if (LOG.isLoggable(Level.FINE)) {
                LOG.info("Returning as server is not up ");
            }
            return false;
        }
        try {
           
            if (myZooKeeper.exists(path, false) == null) {
                myZooKeeper.create(path,
                                   null,
                                   Ids.OPEN_ACL_UNSAFE,
                                   CreateMode.PERSISTENT);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.info("Created persistent " + path + " to zoo keeper");
                }
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
            
            createPersistentPath(treePath);
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
     *  A helper method to add {@link Watcher} to the zkNode at 
     *  the specified path.
     */
    public boolean addWatch(String path, Watcher watcher)
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
    public boolean addWatchToLockNode(String path, Watcher watcher)
    {
        try {
            String lockPath = createLockNode(path);
            myZooKeeper.getChildren(lockPath, watcher);
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
    public boolean addWatchToLockHolder(String path, Watcher watcher)
    {
        try {
            String lockPath = createLockNode(path);
            myZooKeeper.getChildren(lockPath, watcher);
            return true;
        }
        catch (KeeperException | InterruptedException e) {
            return false;
        }
    }
    
    /**
     * Returns true if the number of nodes under a path matches the specified 
     * count.
     */
    public boolean checkLockParticipantsCount(String treePath, int count)
    {
        try {
            String lockPath = 
                treePath + PATH_SEPARATOR + LOCK_NODE;
            Stat stat = myZooKeeper.exists(lockPath, false);
            if (stat == null) {
                return false;
            }
            
            int childCount = stat.getNumChildren();
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Checking the path under " 
                         + lockPath
                         + " and it has " + childCount + " children");
            }
            return childCount == count;
        }
        catch (KeeperException | InterruptedException e) {
            return false;
        }
    }
    
    private String createLockNode(String treePath)
    {
        String path = treePath + PATH_SEPARATOR + LOCK_NODE;
        createPersistentPath(path);
        return path;
    }
    
    private Long parseSequenceNumber(String nodeName)
    {
        return Long.valueOf(nodeName.substring(PARTICIPANTS_NODE.length()));
    }
    
    /**
     * A helper method to acquire lock under a given path using 
     * zookeeper's sequential nodes.
     */
    public boolean acquireLockUnder(String treePath, NodeID nodeID)
    {
        try {
            String path = createLockNode(treePath);
            String createdPath = null;
            Stat stat = new Stat();
            for (String child : myZooKeeper.getChildren(path, false)) {
                String childPath = path + PATH_SEPARATOR + child;
                byte[] data = myZooKeeper.getData(childPath, null, stat);
                NodeID childId = new ServerNodeID(new String(data));
                if (childId.equals(nodeID)) {
                    createdPath = child;
                }
            }
            
            if (createdPath == null) {
                createdPath = 
                    myZooKeeper.create(
                        path + PATH_SEPARATOR + PARTICIPANTS_NODE,
                        nodeID.toString().getBytes(),
                        Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL_SEQUENTIAL);
                
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.info("Created ephimeral " + createdPath + 
                             " to zoo keeper under " + path);
                }
            }
            
            Long id = 
                parseSequenceNumber(
                    createdPath.substring(
                        createdPath.lastIndexOf(PATH_SEPARATOR) + 1));
            if (LOG.isLoggable(Level.FINE)) {
                LOG.info("Acquired lock with  " + id + " for " + nodeID);
            }
            
            SortedSet<Pair<String,Long>> sequenceNumbers = 
                new TreeSet<>(new SeqNodeComparator());
            for (String child : myZooKeeper.getChildren(path, false)) {
                sequenceNumbers.add(
                    new Pair<>(child,
                               parseSequenceNumber(child)));
            }
            return (sequenceNumbers.first().getSecond() == id);
        }
        catch (KeeperException | InterruptedException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * A helper method to return the node that holds a lock under a 
     * path.
     */
    public Pair<NodeID,Long> getLockHolder(String path)
    {
        try {
            String lockPath = path + PATH_SEPARATOR + LOCK_NODE;
            
            SortedSet<Pair<String,Long>> sequenceNumbers = 
                new TreeSet<>(new SeqNodeComparator());
            for (String child : myZooKeeper.getChildren(lockPath, false))
            {
                sequenceNumbers.add(new Pair<>(child,
                                               parseSequenceNumber(child)));
            }
            
            Pair<String,Long> lockNodePath = sequenceNumbers.first();
            String childPath = 
                lockPath + PATH_SEPARATOR + lockNodePath.getFirst();
            byte[] data = myZooKeeper.getData(childPath, null, new Stat());
            
            return new Pair<NodeID,Long>(
                new ServerNodeID(new String(data)),
                lockNodePath.getSecond());
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
                    if (child.equals(LOCK_NODE)) {
                        continue;
                    }
                    servers.add(new ServerNodeID(child));
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
        return myZooKeeper.getState().isAlive();
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
