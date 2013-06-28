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

package org.hit.node;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hit.communicator.NodeID;
import org.hit.db.model.Schema;
import org.hit.event.GossipUpdateEvent;
import org.hit.gossip.Gossip;
import org.hit.messages.Allocation;
import org.hit.messages.Heartbeat;
import org.hit.partitioner.Partitioner;
import org.hit.util.Pair;

import com.google.inject.Inject;

import gnu.trove.iterator.TObjectDoubleIterator;
import gnu.trove.map.hash.TObjectDoubleHashMap;
import gnu.trove.map.hash.TObjectLongHashMap;
import gnu.trove.procedure.TObjectLongProcedure;

/**
 * Defines the contract for a simple allocator that allocates the key space of
 * a table to the incoming node.
 * 
 * @author Balraja Subbiah
 */
public class StandardAllocator implements Allocator
{
    private final Map<NodeID, TObjectLongHashMap<String>> myNodeToRowCountMap;
    
    private final Map<String, TObjectDoubleHashMap<NodeID>> myTableFillRateMap;
    
    private final Map<String, Schema> myTableToSchemaMap;
    
    private final Map<String, Partitioner<?, ?>> myTableToPartitionMap;
    
    private final NodeConfig myNodeConfig;
    
    private final Set<NodeID> myNodes;
    
    private final NodeID myServerID;

    /**
     * CTOR
     */
    @Inject
    public StandardAllocator(NodeConfig nodeConfig, NodeID serverID)
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
        Partitioner<?,?> partitioner = 
                        tableSchema.getKeyspace().makePartitioner(tableSchema.getTableName());
        partitioner.update(new Pair<Comparable<?>, NodeID>(
            tableSchema.getKeyspace().getDomain().getMaximum(),
            myServerID));
        myTableToPartitionMap.put(tableSchema.getTableName(), partitioner);
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
    
    private void updateFillRate(final NodeID nodeID, 
                                final TObjectLongHashMap<String>   
                                    oldNodeRowCountMap,
                                final TObjectLongHashMap<String>   
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
                
                TObjectDoubleHashMap<NodeID> nodeFillRate = 
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
    public Allocation getAllocation(NodeID nodeID) throws IllegalAccessException
    {
        if (!myNodeConfig.isMaster()) {
            throw new IllegalAccessException(
                "Trying get allocation from a non master node");
        }
        
        Map<String, Schema> tableSchemaMap = new HashMap<>();
        Map<String, Partitioner<?,?>> partitionTableMap = new HashMap<>();
        for (Map.Entry<String, Schema> entry : myTableToSchemaMap.entrySet())
        {
            tableSchemaMap.put(entry.getKey(), entry.getValue());
            NodeID maxLoadedNode = lookupMaxLoadedNode(entry.getKey());
            Partitioner<?, ?> partition = 
                myTableToPartitionMap.get(entry.getKey());
            Pair<?,?> nodeRange = partition.getNodeRange(maxLoadedNode);
            Comparable<?> newValue = 
                entry.getValue().getKeyspace().getDomain().getMiddleOf(
                    nodeRange.getFirst(), nodeRange.getSecond());
            partition.update(new Pair<Comparable<?>, NodeID>(newValue, nodeID));
            partitionTableMap.put(entry.getKey(), partition);
        }
        return new Allocation(tableSchemaMap, partitionTableMap);
    }
    
    /**
     * A helper method to lookup a node whose partition is filling fast.
     */
    private NodeID lookupMaxLoadedNode(String tableName)
    {
        TObjectDoubleHashMap<NodeID> tableFillRateMap = 
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

    /**
     * {@inheritDoc}
     */
    @Override
    public GossipUpdateEvent getGossipUpdates()
    {
        return new GossipUpdateEvent(new ArrayList<Gossip>(
            myTableToPartitionMap.values()));
    }
}
