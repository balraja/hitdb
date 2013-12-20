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

import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.map.TObjectDoubleMap;
import gnu.trove.map.TObjectLongMap;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.procedure.TObjectLongProcedure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.hit.communicator.NodeID;
import org.hit.db.keyspace.domain.DiscreteDomain;
import org.hit.db.model.Schema;
import org.hit.db.partitioner.Partitioner;
import org.hit.event.DBStatEvent;
import org.hit.event.GossipUpdateEvent;
import org.hit.gossip.Gossip;
import org.hit.messages.Allocation;
import org.hit.messages.Heartbeat;
import org.hit.server.ServerConfig;
import org.hit.util.LogFactory;
import org.hit.util.Pair;
import org.hit.util.Range;

import com.google.inject.Inject;

/**
 * Defines the contract for a simple allocator that allocates the key space of
 * a table to the incoming node.
 *
 * @author Balraja Subbiah
 */
public class StandardAllocator implements Allocator
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(StandardAllocator.class);

    private final ServerConfig myNodeConfig;

    private final Set<NodeID> myNodes;

    private final Map<NodeID, TObjectLongMap<String>> myNodeToRowCountMap;

    private final NodeID myServerID;

    private final Map<String, TObjectDoubleMap<NodeID>> myTableFillRateMap;

    private final Map<String, Partitioner<?, ?>> myTableToPartitionMap;

    private final Map<String, Schema> myTableToSchemaMap;

    /**
     * CTOR
     */
    @Inject
    public StandardAllocator(ServerConfig nodeConfig, NodeID serverID)
    {
        super();
        myNodeConfig = nodeConfig;
        myTableFillRateMap = new HashMap<>();
        myNodeToRowCountMap = new HashMap<>();
        myTableToSchemaMap = new HashMap<>();
        myTableToPartitionMap = new HashMap<>();
        myNodes = new HashSet<>();
        myServerID = serverID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addSchema(Schema tableSchema)
    {
        myTableToSchemaMap.put(tableSchema.getTableName(), tableSchema);
        
        if (!tableSchema.isReplicated()) {
            return;
        }
        
        Partitioner<?,?> partitioner =
            tableSchema.getKeyspace().makePartitioner(
                tableSchema.getTableName());
        
        DiscreteDomain<?> domain = 
            tableSchema.getKeyspace().getDomain();
        
        long perNodeRange = domain.getTotalElements() / (myNodes.size() + 1);
        int i = 1;
        for (NodeID node : myNodes) {
            partitioner.update(new Pair<Comparable<?>, NodeID>(
                domain.elementAt(i * perNodeRange),
                node));
            
            LOG.info("Adding " + myServerID + " to handle data upto " +
                     tableSchema.getKeyspace().getDomain().getMaximum());
            i++;
        }
        
        // Allocate the remaining keys to the master node.
        partitioner.update(new Pair<Comparable<?>, NodeID>(
            domain.getMaximum(),
            myServerID));
        
        LOG.info("Adding " + myServerID + " to handle data upto " +
            tableSchema.getKeyspace().getDomain().getMaximum());

        myTableToPartitionMap.put(tableSchema.getTableName(), partitioner);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Allocation getAllocation(NodeID nodeID) throws IllegalAccessException
    {
        if (!myNodeConfig.isMaster()) {
            throw new IllegalAccessException(
                "Trying get allocation from a non master node");
        }

        Map<String, Schema> tableSchemaMap = new HashMap<>();
        Map<String, Partitioner<?,?>> partitionTableMap = new HashMap<>();
        Map<String, NodeID> dataNodeMap = new HashMap<>();
        for (Map.Entry<String, Schema> entry : myTableToSchemaMap.entrySet())
        {
            tableSchemaMap.put(entry.getKey(), entry.getValue());
            if (myNodes.isEmpty() || myNodeToRowCountMap.isEmpty()) {
                // We don't have information about other nodes to make
                // a partitioning decision. Hencejust add the schemas and 
                // return.
                continue;
            }
            if (entry.getValue().isReplicated()) {
                
                Partitioner<?,?> partition =
                    entry.getValue().getKeyspace().makePartitioner(
                        entry.getValue().getTableName());
                
                DiscreteDomain<?> domain = 
                    entry.getValue().getKeyspace().getDomain();
                
                partition.update(new Pair<Comparable<?>, NodeID>(
                    domain.getMaximum(), nodeID));
                
                dataNodeMap.put(entry.getKey(), myServerID);
            }
            else {
                NodeID maxLoadedNode = lookupMaxLoadedNode(entry.getKey());
                Partitioner<?, ?> partition =
                    myTableToPartitionMap.get(entry.getKey());
                Range<?> nodeRange = partition.getNodeRange(maxLoadedNode);
                Comparable<?> newValue =
                    entry.getValue().getKeyspace().getDomain().getMiddleOf(
                        nodeRange.getMinValue(), nodeRange.getMaxValue());
                partition.update(new Pair<Comparable<?>, NodeID>(newValue, nodeID));
                partitionTableMap.put(entry.getKey(), partition);
                dataNodeMap.put(entry.getKey(), maxLoadedNode);
            }
        }
        myNodes.add(nodeID);
        return new Allocation(tableSchemaMap, partitionTableMap, dataNodeMap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GossipUpdateEvent getGossipUpdates()
    {
        return new GossipUpdateEvent(new ArrayList<Gossip>(
            myTableToPartitionMap.values()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Partitioner<?, ?>> getPartitions()
    {
        return new HashMap<>(myTableToPartitionMap);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void listenTO(DBStatEvent dbStats)
    {
        updateFillRate(myServerID,
                       myNodeToRowCountMap.get(myServerID),
                       dbStats.getTableToRowCountMap());
        myNodeToRowCountMap.put(myServerID, dbStats.getTableToRowCountMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void listenTO(NodeID nodeID, Heartbeat heartbeat)
    {
        myNodes.add(nodeID);
        updateFillRate(nodeID,
                       myNodeToRowCountMap.get(nodeID),
                       heartbeat.getTableToRowCountMap());
        myNodeToRowCountMap.put(nodeID, heartbeat.getTableToRowCountMap());
    }

    /**
     * A helper method to lookup a node whose partition is filling fast.
     */
    private NodeID lookupMaxLoadedNode(String tableName)
    {
        TObjectDoubleMap<NodeID> tableFillRateMap =
            myTableFillRateMap.get(tableName);
        TObjectDoubleIterator<NodeID> itr = tableFillRateMap.iterator();
        NodeID maxLoadedNode = null;
        double fillRate = 0.0;
        while (itr.hasNext()) {
            if (itr.value() > fillRate) {
                maxLoadedNode = itr.key();
                fillRate = itr.value();
            }
            itr.advance();
        }
        return maxLoadedNode;
    }

    private void updateFillRate(final NodeID nodeID,
                                final TObjectLongMap<String>
                                    oldNodeRowCountMap,
                                final TObjectLongMap<String>
                                    nodeRowCountMap)
    {
        nodeRowCountMap.forEachEntry(new TObjectLongProcedure<String>()
        {
            @Override
            public boolean execute(String tableName, long rowCount)
            {
                long oldRowCount = oldNodeRowCountMap != null ?
                                       oldNodeRowCountMap.get(tableName) : 0;
                double fillRate =
                    ((double) (rowCount - oldRowCount)) / ((double) oldRowCount);

                TObjectDoubleMap<NodeID> nodeFillRate =
                    myTableFillRateMap.get(tableName);
                if (nodeFillRate == null) {
                    nodeFillRate = new TObjectDoubleHashMap<>();
                    myTableFillRateMap.put(tableName, nodeFillRate);
                }
                nodeFillRate.put(nodeID, fillRate);
                return true;
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(Set<NodeID> nodes)
    {
        myNodes.addAll(nodes);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<NodeID> getMonitoredNodes()
    {
        return Collections.unmodifiableSet(myNodes);
    }
}
