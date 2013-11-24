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
package org.hit.server;

import org.hit.util.ApplicationProperties;

/**
 * Implements {@link ServerConfig} by extracting out the values from property.
 * 
 * @author Balraja Subbiah
 */
public  class ServerPropertyConfig implements ServerConfig
{
    public static final String SERVER_NAME_PROPERTY = 
        "org.hit.server.name";
    
    public static final String SERVER_REPLICATION_FACTOR_PROPERTY = 
        "org.hit.server.replicationFactor";
    
    public static final String REPLICATION_SLAVE_FOR_PROPERTY = 
        "org.hit.server.replicatingDataForGroup";
    
    public static final String SERVER_COUNT_PROPERTY = 
        "org.hit.server.count";
    
    public static final String IS_MASTER_PROPERTY = 
        "org.hit.server.isMaster";
    
    public static final String HEARTBEAT_INTERVAL_PROPERTY= 
        "org.hit.server.heartbeatIntervalInSecs";
    
    public static final String GOSSIP_INTERVAL_PROPERTY= 
        "org.hit.server.gossipIntervalInSecs";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getServerName()
    {
        return ApplicationProperties.getProperty(SERVER_NAME_PROPERTY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInitialServerCount()
    {
        return Integer.parseInt(
            ApplicationProperties.getProperty(SERVER_COUNT_PROPERTY));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getReplicationFactor()
    {
        return Integer.parseInt(
            ApplicationProperties.getProperty(
                SERVER_REPLICATION_FACTOR_PROPERTY));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getReplicationGroup()
    {
        return ApplicationProperties.getProperty(REPLICATION_SLAVE_FOR_PROPERTY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMaster()
    {
        return Boolean.valueOf(
            ApplicationProperties.getProperty(IS_MASTER_PROPERTY));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHeartBeatIntervalSecs()
    {
        return Integer.parseInt(
            ApplicationProperties.getProperty(HEARTBEAT_INTERVAL_PROPERTY));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getGossipUpdateSecs()
    {
        return Integer.parseInt(
            ApplicationProperties.getProperty(GOSSIP_INTERVAL_PROPERTY));
    }

}
