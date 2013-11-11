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

package org.hit.di;

import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.consensus.ConsensusProtocolProvider;
import org.hit.consensus.paxos.PaxosProvider;
import org.hit.consensus.raft.log.WALConfig;
import org.hit.consensus.raft.log.WALPropertyConfig;
import org.hit.db.engine.Allocator;
import org.hit.db.engine.EngineConfig;
import org.hit.db.engine.EnginePropertyConfig;
import org.hit.db.engine.EngineWarden;
import org.hit.db.engine.StandardAllocator;
import org.hit.db.engine.TransactionManager;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.impl.TransactableHitDatabase;
import org.hit.fs.FileSystemFacacde;
import org.hit.fs.StandardFileSystem;
import org.hit.time.Clock;
import org.hit.time.SimpleSystemClock;

import com.google.inject.Provides;

/**
 * Extends <code>HitModule</code> to support adding bindings for the server
 * side.
 *
 * @author Balraja Subbiah
 */
public class HitServerModule extends HitModule
{
    private final EventBus myEventBus;

    /**
     * CTOR
     */
    public HitServerModule()
    {
        myEventBus = new EventBus();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure()
    {
        super.configure();
        bind(ConsensusProtocolProvider.class).to(PaxosProvider.class);
        bind(Clock.class).to(SimpleSystemClock.class);
        bind(WALConfig.class).to(WALPropertyConfig.class);
        bind(FileSystemFacacde.class).to(StandardFileSystem.class);
        bind(EngineConfig.class).to(EnginePropertyConfig.class);
        bind(Allocator.class).to(StandardAllocator.class);
        bind(TransactableDatabase.class).to(TransactableHitDatabase.class);
        bind(TransactionManager.class).toProvider(
            TransactionManagerProvider.class);
        bind(EngineWarden.class).toProvider(EngineWardenProvider.class);
        bind(NodeID.class).toProvider(ServerIDProvider.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getDefaultBoundPort()
    {
        return Integer.valueOf(10000);
    }

    @Provides
    protected EventBus getEventBus()
    {
        return myEventBus;
    }
}
