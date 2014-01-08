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

package org.hit.db.engine;

import java.util.Map;
import java.util.Set;

import org.hit.communicator.NodeID;
import org.hit.db.model.HitTableSchema;
import org.hit.db.partitioner.Partitioner;
import org.hit.event.DBStatEvent;
import org.hit.event.GossipUpdateEvent;
import org.hit.messages.Allocation;
import org.hit.messages.Heartbeat;

/**
 * Defines an interface that can be used for allocating the keyspace to
 * various nodes.
 *
 * @author Balraja Subbiah
 */
public interface Allocator
{
    /**
     * The schema to be monitored.
     */
    public void addSchema(HitTableSchema tableSchema);
    
    /**
     * Initializes the allocator with the list of other hit database 
     * servers.
     */
    public void initialize(Set<NodeID> nodes);

    /**
     * Returns the <code>Allocation</code> corresponding to the given node.
     */
    public Allocation getAllocation(NodeID nodeID) throws IllegalAccessException;

    /**
     * Returns an <code>GossipUpdateEvent</code> to get updates about
     * the latest keyspace partitions.
     */
    public GossipUpdateEvent getGossipUpdates();

    /**
     * Returns the {@link Partitioner}s for various tables.
     */
    public Map<String, Partitioner<?,?>>  getPartitions();

    /**
     * Updates the statistics published by the db server running on the
     * master node.
     */
    public void listenTO(DBStatEvent dbStats);

    /**
     * Listens to heartbeats from  the given <code>NodeID</code>.
     */
    public void listenTO(NodeID nodeID, Heartbeat heartbeat);
    
    /**
     * Returns the list of slave server nodes that are monitored by this master 
     * node.
     */
    public Set<NodeID> getMonitoredNodes();
}
