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

import org.hit.buffer.BufferConfig;
import org.hit.buffer.BufferManager;
import org.hit.buffer.BufferPropertyConfig;
import org.hit.communicator.Communicator;
import org.hit.communicator.MessageSerializer;
import org.hit.communicator.ObjectStreamSerializer;
import org.hit.communicator.ObjectStreamSerializerFactory;
import org.hit.communicator.SerializerFactory;
import org.hit.communicator.nio.NIOCommunicator;
import org.hit.gms.GroupID;
import org.hit.gms.SimpleGroupID;
import org.hit.util.ApplicationProperties;
import org.hit.zookeeper.ZooKeeperClient;
import org.hit.zookeeper.ZooKeeperClientConfig;
import org.hit.zookeeper.ZooKeeperClientPropertyConfig;

import com.google.inject.AbstractModule;
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
    public static final String HIT_COMM_PORT_PROPERTY = 
        "org.hit.communicator.port";
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure()
    {
        bindConstant().annotatedWith(Names.named("PreferredPort"))
                      .to(getBoundPort());
        
        bind(SerializerFactory.class).to(ObjectStreamSerializerFactory.class);
        bind(Communicator.class).to(NIOCommunicator.class);
        bind(ZooKeeperClientConfig.class)
            .to(ZooKeeperClientPropertyConfig.class);
        bind(ZooKeeperClient.class).toProvider(ZookeeperClientProvider.class);
        bind(String.class).annotatedWith(Names.named("ServerGroupName"))
                          .toInstance("HitServers");
        bind(BufferConfig.class).to(BufferPropertyConfig.class);
    }

    protected Integer getBoundPort()
    {
        String boundPort = 
            ApplicationProperties.getProperty(HIT_COMM_PORT_PROPERTY);
        return boundPort != null ? Integer.valueOf(boundPort)
                                 : getDefaultBoundPort();
    }
    
    @Named("ServerGroupID")
    @Provides
    GroupID makeServerGroupID(@Named("ServerGroupName") String serverGroupName)
    {
        return new SimpleGroupID(serverGroupName);
    }
    
    @Named("communicator")
    @Provides
    BufferManager makeCommunicatorBufferManager(
        BufferConfig config)
    {
        return new BufferManager(Communicator.class.getSimpleName().toLowerCase(), 
                                 config);
    }

    /**
     * Sub classes should override this method to provide a 
     * default value for communicator port.
     */
    protected abstract Integer getDefaultBoundPort();
}
