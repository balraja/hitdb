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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Schema;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.journal.WAL;
import org.hit.event.Event;
import org.hit.event.ProposalNotificationEvent;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.DBOperationMessage;
import org.hit.messages.DistributedDBOperationMessage;
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

    private static final String TABLE_CREATION_LOG =
        "Received request from %s for creating table %s";
    
    private static final String DB_OPERATION_LOG =
        "Received request from %s for performing %s";

    private TransactionManager myTransactionManager;

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

        TransactableDatabase dataStore = new TransactableHitDatabase();

        myTransactionManager =
            new TransactionManager(
                dataStore,
                clock,
                eventBus,
                nodeID,
                wal,
                zooKeeperClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Received " + event);
        }
        if (event instanceof CreateTableMessage) {
            CreateTableMessage ctm = (CreateTableMessage) event;
            LOG.info(String.format(TABLE_CREATION_LOG,
                                   ctm.getNodeId(),
                                   ctm.getTableSchema()));
            Schema schema = ctm.getTableSchema();
            myTransactionManager.createTable(ctm.getNodeId(), schema);
        }
        else if (event instanceof DBOperationMessage) {

            DBOperationMessage message =
                (DBOperationMessage) event;
            
            LOG.info(String.format(DB_OPERATION_LOG,
                                   message.getNodeId(),
                                   message.getOperation()));
            DBOperation operation =
                ((DBOperationMessage) event).getOperation();
            myTransactionManager.processOperation(
                message.getNodeId(), operation);
        }
        else if (event instanceof DistributedDBOperationMessage) {
            
            DistributedDBOperationMessage ddbMessage = 
                (DistributedDBOperationMessage) event;
            
            LOG.info(String.format(DB_OPERATION_LOG,
                                   ddbMessage.getNodeId(),
                                   ddbMessage.getNodeToOperationMap().keySet()));
            
            myTransactionManager.processOperation(
                ddbMessage.getNodeId(), 
                ddbMessage.getNodeToOperationMap());
        }
        else if (event instanceof ProposalNotificationEvent) {
            ProposalNotificationEvent pne = (ProposalNotificationEvent) event;
            if (pne.getProposal() instanceof DistributedTrnProposal) {
                
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(CreateTableMessage.class,
                                       getActorID());
        getEventBus().registerForEvent(DBOperationMessage.class,
                                       getActorID());
    }

  }
