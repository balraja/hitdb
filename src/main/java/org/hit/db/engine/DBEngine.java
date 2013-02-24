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

package org.hit.db.engine;

import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Schema;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.journal.WAL;
import org.hit.event.ApplyToReplicaEvent;
import org.hit.event.CreateConsensusAcceptorEvent;
import org.hit.event.CreateConsensusLeaderEvent;
import org.hit.event.CreateTableMessage;
import org.hit.event.Event;
import org.hit.event.PerformDBOperationMessage;
import org.hit.time.Clock;
import org.hit.topology.Topology;
import org.hit.util.LogFactory;
import org.hit.zookeeper.ZooKeeperClient;

import com.google.inject.Inject;

/**
 * Implements the database engine that's reponsible for creating tables and
 * responding to queries.
 * 
 * @author Balraja Subbiah
 */
public class DBEngine extends Actor
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(DBEngine.class);
    
    private final TransactionManager myTransactionManager;
    
    private final ReplicationManager myReplicationManager;
    
    private final NodeID myNodeID;
    
    private final NodeID myReplicatingOnNodeID;
    
    /**
     * CTOR
     */
    @Inject
    public DBEngine(EventBus             eventBus,
                    NodeID               nodeID,
                    Clock                clock,
                    WAL                  wal,
                    Topology             topology,
                    ZooKeeperClient      zooKeeperClient)
    {
        super(eventBus, new ActorID(DBEngine.class.getName()));
        
        TransactableDatabase dataStore =
            new TransactableHitDatabase(nodeID);
        
        myNodeID = nodeID;
        myReplicatingOnNodeID = topology.getReplicatingNodeID(myNodeID);
        NodeID replicaSource = topology.getReplicaSource(myNodeID);
        if (replicaSource != null) {
            myReplicationManager = 
                new ReplicationManager(replicaSource);
        }
        else {
            myReplicationManager = null;
        }
        
        myTransactionManager =
            new TransactionManager(
                dataStore, 
                clock, 
                eventBus, 
                nodeID, 
                wal, 
                zooKeeperClient,
                myReplicatingOnNodeID != null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof CreateTableMessage) {
            CreateTableMessage ctm = (CreateTableMessage) event;
            Schema schema = ctm.getTableSchema();
            myTransactionManager.createTable(ctm.getNodeId(), schema);
        }
        else if (event instanceof PerformDBOperationMessage) {
            PerformDBOperationMessage message =
                (PerformDBOperationMessage) event;
            DBOperation operation =
                ((PerformDBOperationMessage) event).getOperation();
            myTransactionManager.processOperation(
                message.getNodeId(), operation);
        }
        else if (event instanceof ApplyToReplicaEvent 
                 && myReplicationManager != null) 
        {
            ApplyToReplicaEvent applyToReplicaEvent =
                (ApplyToReplicaEvent) event;
            myReplicationManager.applyReplicationProposal(
                (DBReplicaProposal) applyToReplicaEvent.getProposal());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        super.start();
        try {
            if (myReplicatingOnNodeID != null) {
                getEventBus().publish(
                    new CreateConsensusLeaderEvent(
                        new ReplicatedDatabaseID(myNodeID),
                        Collections.singleton(myReplicatingOnNodeID)));
            }
            
            if (myReplicationManager != null) {
                getEventBus().publish(
                    new CreateConsensusAcceptorEvent(
                         new ReplicatedDatabaseID(
                             myReplicationManager.getReplicatedDbsNode()),
                         myReplicationManager.getReplicatedDbsNode()));
            }
        }
        catch (EventBusException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
