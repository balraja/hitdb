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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.db.model.DBOperation;
import org.hit.db.model.Schema;
import org.hit.db.model.mutations.SingleKeyMutation;
import org.hit.event.Event;
import org.hit.event.ProposalNotificationEvent;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.DBOperationMessage;
import org.hit.messages.DistributedDBOperationMessage;
import org.hit.partitioner.Partitioner;
import org.hit.util.LogFactory;

/**
 * An abstract implementation of <code>EngineWarden</code> that supports
 * database operations.
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractWarden implements EngineWarden
{
    private static final String DB_OPERATION_LOG =
        "Received request from %s for performing %s";

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(DBEngine.class);

    private final TransactionManager myTransactionManager;
    
    private final EngineConfig myEngineConfig;
    
    private final EventBus myEventBus;
    
    /**
     * CTOR
     */
    public AbstractWarden(TransactionManager transactionManager,
                          EngineConfig       engineConfig,
                          EventBus           eventBus)
    {
        myTransactionManager = transactionManager;
        myEngineConfig = engineConfig;
        myEventBus = eventBus;
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
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void register(ActorID actorID)
    {
        myEventBus.registerForEvent(DBOperationMessage.class,
                                    actorID);
    }

    /**
     * Returns the value of transactionManager
     */
    protected TransactionManager getTransactionManager()
    {
        return myTransactionManager;
    }

    /**
     * Returns the value of engineConfig
     */
    protected EngineConfig getEngineConfig()
    {
        return myEngineConfig;
    }

    /**
     * Returns the value of eventBus
     */
    protected EventBus getEventBus()
    {
        return myEventBus;
    }    
}
