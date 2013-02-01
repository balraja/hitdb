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

import org.hit.communicator.Communicator;
import org.hit.communicator.MessageSerializer;
import org.hit.communicator.NodeID;
import org.hit.communicator.SerializableSerializer;
import org.hit.communicator.nio.IPNodeID;
import org.hit.communicator.nio.NIOCommunicator;
import org.hit.zookeeper.ZooKeeperClient;
import org.hit.zookeeper.ZooKeeperClientConfig;
import org.hit.zookeeper.ZooKeeperClientPropertyConfig;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

/**
 * Extends {@link AbstractModule} to support injecting the necessary dependency
 * injection parameters.
 * 
 * @author Balraja Subbiah
 */
public abstract class HitModule extends AbstractModule
{
    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure()
    {
        bind(Integer.class).annotatedWith(Names.named("PreferredPort"))
                           .toInstance(getBoundPort());

        bind(MessageSerializer.class).to(SerializableSerializer.class);
        bind(Communicator.class).to(NIOCommunicator.class);
        bind(ZooKeeperClientConfig.class)
            .to(ZooKeeperClientPropertyConfig.class);
    }
    
    protected abstract Integer getBoundPort();
    
    @Provides
    protected NodeID provideNodeID(@Named("PreferredPort") Integer port)
    {
        return new IPNodeID(port);
    }
    
    @Provides
    protected ZooKeeperClient provideZookeeperClient(
        ZooKeeperClientConfig config)
    {
        return new ZooKeeperClient(config);
    }
}
