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
import org.hit.consensus.UnitID;
import org.hit.db.engine.Allocator;
import org.hit.db.engine.EngineWarden;
import org.hit.db.engine.ReplicationID;
import org.hit.db.engine.StandardAllocator;
import org.hit.db.engine.TransactionManager;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.db.transactions.impl.TransactableHitDatabase;
import org.hit.fs.FileSystemFacacde;
import org.hit.fs.StandardFileSystem;
import org.hit.gms.GroupID;
import org.hit.gms.SimpleGroupID;
import org.hit.server.ServerConfig;
import org.hit.server.ServerPropertyConfig;
import org.hit.time.Clock;
import org.hit.time.SimpleSystemClock;

import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

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
     * CTOR
     */
    public HitServerModule(EventBus eventBus)
    {
        myEventBus = eventBus;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure()
    {
        super.configure();
        bind(Clock.class).to(SimpleSystemClock.class);
        bind(FileSystemFacacde.class).to(StandardFileSystem.class);
        bind(Allocator.class).to(StandardAllocator.class);
        bind(TransactableDatabase.class).to(TransactableHitDatabase.class);
        bind(TransactionManager.class).toProvider(
            TransactionManagerProvider.class);
        bind(EngineWarden.class).toProvider(EngineWardenProvider.class);
        bind(ServerConfig.class).to(ServerPropertyConfig.class);
        bind(NodeID.class).toProvider(ServerIDProvider.class);
        bind(String.class).annotatedWith(Names.named("ServerGroupName"))
                          .toInstance("HitServers");
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
    
    @Named("ReplicationUnitID")
    @Provides
    public UnitID makeReplicationUnitID(ServerConfig config)
    {
        return new ReplicationID(config.getServerName());
    }
    
    @Named("ReplicationSlaveUnitID")
    @Provides
    UnitID makeReplicationSlaveUnitID(ServerConfig config)
    {
        return new ReplicationID(config.getReplicationGroup());
    }
    
    @Named("ServerGroupID")
    @Provides
    GroupID makeServerGroupID(@Named("ServerGroupName") String serverGroupName)
    {
        return new SimpleGroupID(serverGroupName);
    }
    
    @Named("ReplicationSlaveGroupID")
    @Provides
    GroupID makeReplicatingGroupClientID(ServerConfig config)
    {
        return new SimpleGroupID(config.getReplicationGroup());
    }
    
    @Named("ReplicationGroupID")
    @Provides
    GroupID makeReplicatingGroupID(ServerConfig config)
    {
        return new SimpleGroupID(config.getServerName());
    }
}
