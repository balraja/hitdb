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

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.db.model.DBOperation;
import org.hit.event.ConsensusResponseEvent;
import org.hit.event.DBStatEvent;
import org.hit.event.Event;
import org.hit.event.ProposalNotificationEvent;
import org.hit.event.SendMessageEvent;
import org.hit.messages.DBOperationFailureMessage;
import org.hit.messages.DBOperationMessage;
import org.hit.messages.DataLoadRequest;
import org.hit.messages.DistributedDBOperationMessage;
import org.hit.pool.PooledObjects;
import org.hit.server.ServerConfig;
import org.hit.util.LogFactory;

/**
 * An abstract implementation of <code>EngineWarden</code> that supports
 * database operations.
 *
 * @author Balraja Subbiah
 */
public abstract class AbstractJanitor implements EngineJanitor
{
    private static final String DB_OPERATION_LOG =
        "Received request from %s for performing %s";

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(DBEngine.class);

    private final ServerConfig myServerConfig;

    private final EventBus myEventBus;

    private final TransactionManager myTransactionManager;
    
    private final AtomicBoolean myIsInitialized;
    
    private final NodeID myServerID;
    
    /**
     * CTOR
     */
    public AbstractJanitor(TransactionManager transactionManager,
                          ServerConfig       serverConfig,
                          EventBus           eventBus,
                          NodeID             serverID)
    {
        myTransactionManager = transactionManager;
        myServerConfig = serverConfig;
        myEventBus = eventBus;
        myIsInitialized = new AtomicBoolean(false);
        myServerID = serverID;
    }

    /**
     * Returns the value of {@link ServerConfig}.
     */
    protected ServerConfig getServerConfig()
    {
        return myServerConfig;
    }

    /**
     * Returns the value of eventBus
     */
    protected EventBus getEventBus()
    {
        return myEventBus;
    }

    /**
     * Returns the value of transactionManager
     */
    protected TransactionManager getTransactionManager()
    {
        return myTransactionManager;
    }
    
    /**
     * Returns the value of isInitialized
     */
    protected AtomicBoolean getIsInitialized()
    {
        return myIsInitialized;
    }
    
    /**
     * Returns the value of serverID
     */
    protected NodeID getServerID()
    {
        return myServerID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleEvent(Event event)
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Received " + event);
        }

        if (event instanceof DBOperationMessage) {
            DBOperationMessage message = (DBOperationMessage) event;
            if (myIsInitialized.get()) {
                LOG.info(String.format(DB_OPERATION_LOG,
                                       message.getSenderId(),
                                       message.getOperation()));
                DBOperation operation =
                    ((DBOperationMessage) event).getOperation();
                myTransactionManager.processOperation(
                    message.getSenderId(), operation, message.getSequenceNumber());
                PooledObjects.freeInstance(message);
            }
            else {
                myEventBus.publish(
                    ActorID.DB_ENGINE,
                    PooledObjects.getInstance(SendMessageEvent.class).initialize(
                        message.getSenderId(),
                        PooledObjects
                            .getInstance(DBOperationFailureMessage.class)
                            .initialize(
                                myServerID, 
                                message.getSequenceNumber(),
                                "DB not yet initialized")));
            }
        }
        else if (event instanceof DistributedDBOperationMessage) {

            DistributedDBOperationMessage ddbMessage =
                (DistributedDBOperationMessage) event;

            LOG.info(String.format(DB_OPERATION_LOG,
                                   ddbMessage.getSenderId(),
                                   ddbMessage.getNodeToOperationMap().keySet()));
            if (myIsInitialized.get()) {
                myTransactionManager.processOperation(
                    ddbMessage.getSenderId(),
                    ddbMessage.getSequenceNumber(),
                    ddbMessage.getNodeToOperationMap());
            }
            else {
                myEventBus.publish(
                    ActorID.DB_ENGINE,
                    PooledObjects.getInstance(SendMessageEvent.class).initialize(
                        ddbMessage.getSenderId(),
                        PooledObjects
                            .getInstance(DBOperationFailureMessage.class)
                            .initialize(
                                myServerID, 
                                ddbMessage.getSequenceNumber(),
                                "DB not yet initialized")));
            }
        }
        else if (event instanceof ProposalNotificationEvent) {
            ProposalNotificationEvent pne = 
                (ProposalNotificationEvent) event;
            myTransactionManager.processOperation(pne);
        }
        else if (event instanceof ConsensusResponseEvent) {
            ConsensusResponseEvent cre = (ConsensusResponseEvent) event;
            myTransactionManager.processOperation(cre);
        }
        else if (event instanceof DataLoadRequest) {
            DataLoadRequest loadRequest = (DataLoadRequest) event;
            myTransactionManager.processQueryAndDeleteOperation(
                loadRequest.getSenderId(), 
                loadRequest.getTableName(),
                loadRequest.getNodeRange());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void register(ActorID actorID)
    {
        myEventBus.registerForEvent(DBOperationMessage.class, actorID);
        myEventBus.registerForEvent(DistributedDBOperationMessage.class, actorID);
        myEventBus.registerForEvent(ProposalNotificationEvent.class, actorID);
        myEventBus.registerForEvent(ConsensusResponseEvent.class, actorID);
        myEventBus.registerForEvent(DataLoadRequest.class, actorID);
    }
    
    /**
     * Initializes the {@link TransactionManager} with reference to this janitor.
     */
    public void start()
    {
        myTransactionManager.initialize(this);
    }
}
