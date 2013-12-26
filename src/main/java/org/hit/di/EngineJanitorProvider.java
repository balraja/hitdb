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

package org.hit.di;

import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.db.engine.Allocator;
import org.hit.db.engine.EngineJanitor;
import org.hit.db.engine.SlaveJanitor;
import org.hit.db.engine.MasterJanitor;
import org.hit.db.engine.TransactionManager;
import org.hit.server.ServerConfig;

import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Implements <code>Provider</code> to generate <code>EngineWarden</code>
 *
 * @author Balraja Subbiah
 */
public class EngineJanitorProvider implements Provider<EngineJanitor>
{
    private final EngineJanitor myWarden;

    /**
     * CTOR
     */
    @Inject
    public EngineJanitorProvider(TransactionManager transactionManager,
                                ServerConfig engineConfig,
                                EventBus eventBus,
                                NodeID nodeID,
                                Allocator allocator)
    {
        myWarden = engineConfig.isMaster() ?
                       new MasterJanitor(transactionManager,
                                        engineConfig,
                                        eventBus,
                                        nodeID,
                                        allocator)
                        : new SlaveJanitor(transactionManager,
                                          engineConfig,
                                          eventBus,
                                          nodeID);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EngineJanitor get()
    {
        return myWarden;
    }
}
