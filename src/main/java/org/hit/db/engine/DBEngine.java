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
import org.hit.db.transactions.impl.TransactableHitDatabase;
import org.hit.db.transactions.journal.WAL;
import org.hit.event.Event;
import org.hit.event.ProposalNotificationEvent;
import org.hit.messages.CreateTableMessage;
import org.hit.messages.DBOperationMessage;
import org.hit.messages.DistributedDBOperationMessage;
import org.hit.time.Clock;
import org.hit.util.LogFactory;

import com.google.inject.Inject;

/**
 * Implements the database engine that's reponsible for creating tables and
 * responding to queries.
 *
 * @author Balraja Subbiah
 */
public class DBEngine extends Actor
{
    private final EngineWarden myEngineWarden;
    
    /**
     * CTOR
     */
    @Inject
    public DBEngine(EventBus eventBus, EngineWarden warden)
    {
        super(eventBus, new ActorID(DBEngine.class.getName()));
        myEngineWarden = warden;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        myEngineWarden.handleEvent(event);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        myEngineWarden.register(getActorID());       
    }
  }
