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

import java.util.Set;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.event.Event;
import org.hit.util.Pair;

import com.google.inject.Inject;

/**
 * Implements the database engine that's responsible for creating tables and
 * responding to queries.
 *
 * @author Balraja Subbiah
 */
public class DBEngine extends Actor
{
    private final EngineJanitor myEngineWarden;
    
    /**
     * CTOR
     */
    @Inject
    public DBEngine(EventBus eventBus, EngineJanitor warden)
    {
        super(eventBus, ActorID.DB_ENGINE);
        myEngineWarden = warden;
    }
    
    /**
     * Initializes the <code>DBEngine</code> with the master node.
     */
    public void init(NodeID masterNode)
    {
        if (myEngineWarden instanceof SlaveJanitor) {
            ((SlaveJanitor) myEngineWarden).setMaster(masterNode);
        }
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
    
    /**
     * Starts the database engine.
     */
    public void start(Pair<NodeID,Set<NodeID>> serverGroup)
    {
        super.start();
        if (myEngineWarden instanceof MasterJanitor) {
            ((MasterJanitor) myEngineWarden).start(serverGroup.getSecond());
        }
        else {
            ((SlaveJanitor) myEngineWarden).start(serverGroup.getFirst());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        super.stop();
        myEngineWarden.stop();
    }
  }
