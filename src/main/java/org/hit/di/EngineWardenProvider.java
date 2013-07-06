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
import org.hit.db.engine.EngineConfig;
import org.hit.db.engine.EngineWarden;
import org.hit.db.engine.LocalWarden;
import org.hit.db.engine.MasterWarden;
import org.hit.db.engine.TransactionManager;

import com.google.inject.Provider;

/**
 * Implements <code>Provider</code> to generate <code>EngineWarden</code>
 * 
 * @author Balraja Subbiah
 */
public class EngineWardenProvider implements Provider<EngineWarden>
{
    private final EngineWarden myWarden;
    
    /**
     * CTOR
     */
    public EngineWardenProvider(TransactionManager transactionManager,
                                EngineConfig engineConfig,
                                EventBus eventBus,
                                NodeID nodeID,
                                Allocator allocator)
    {
        myWarden = engineConfig.isMaster() ?
                       new MasterWarden(transactionManager, 
                                        engineConfig, 
                                        eventBus, 
                                        nodeID, 
                                        allocator)
                        : new LocalWarden(transactionManager, 
                                          engineConfig, 
                                          eventBus);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public EngineWarden get()
    {
        return myWarden;
    }
}
