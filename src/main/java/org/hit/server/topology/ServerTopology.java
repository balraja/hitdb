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
package org.hit.server.topology;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

/**
 * Defines the contract for a topology of servers. A topology specifies the
 * following:
 * 
 * <ul>
 * <li>Number of servers</li>
 * <li>Name of each server</li>
 * <li>The names of servers to which server's data is replicated</li>
 * </ul>
 * The default topology will be read from HITDB_HOME/conf/server.topology.xml.
 * 
 * @author Balraja Subbiah
 */
public class ServerTopology
{
    public static final int UNKNOWN_PORT = -1;
    
    private static final String SERVERS_NAME_KEY = "servers.server.name";
    
    private static final int  DEFAULT_REPLICATION_FACTOR = 2; 
    
    private static final String NAME_ATTRIBUTE = "name";
    
    private static final String IS_MASTER_ATTRIBUTE = "is_master";
    
    private static final String REPLICATING_SERVER_ATTRIBUTE = "replicating_server";
    
    private static final String PORT_ATTRIBUTE = "port";
    
    private static final String SERVERS_GROUP_KEY = "servers.server";
    
    private final XMLConfiguration myTopologyConfiguration;
    
    /**
     * CTOR
     * 
     * @throws ConfigurationException 
     */
    public ServerTopology(File fileHandle) throws ConfigurationException 
    {
       this(new XMLConfiguration(fileHandle));
    }
    
    /**
     * CTOR
     * 
     * @throws ConfigurationException 
     */
    public ServerTopology(URL url) throws ConfigurationException 
    {
       this(new XMLConfiguration(url));
    }
    
    /** Private CTOR */
    private ServerTopology(XMLConfiguration topologyConfiguration)
    {
        myTopologyConfiguration = topologyConfiguration;
    }
    
    /** Returns the names of servers listed in the topology */
    @SuppressWarnings("unchecked")
    public List<String> getServers()
    {
        return (List<String>)
                myTopologyConfiguration.getList(SERVERS_NAME_KEY, 
                                                Collections.EMPTY_LIST);
    }
    
    /**
     * Returns the replication factor for given list of servers.
     */
    public int getReplicationFactor()
    {
        return DEFAULT_REPLICATION_FACTOR;
    }
    
    private HierarchicalConfiguration getServerConfiguration(String name)
    {
        @SuppressWarnings("unchecked")
        List<HierarchicalConfiguration> serverConfigs = 
           (List<HierarchicalConfiguration>)
                myTopologyConfiguration.configurationsAt(SERVERS_GROUP_KEY);
        for (HierarchicalConfiguration serverConfig : serverConfigs)
        {
            String serverName = serverConfig.getString(NAME_ATTRIBUTE);
            if (   serverName != null 
                && !serverName.isEmpty()
                && serverName.equalsIgnoreCase(name)) 
            {
                return serverConfig;
            }
        }
        return null;
    }
    
    /** Returns true if the given server will act as master */
    public boolean isMasterServer(String serverName)
    {
        HierarchicalConfiguration serverConfig = 
            getServerConfiguration(serverName);
        
        return serverConfig != null ? 
            serverConfig.getBoolean(IS_MASTER_ATTRIBUTE)
            : false;
    }
    
    /** Returns the port to which a server will be bound */
    public int getPort(String serverName)
    {
        HierarchicalConfiguration serverConfig = 
            getServerConfiguration(serverName);
        
        return serverConfig != null ? 
            serverConfig.getInt(PORT_ATTRIBUTE) : UNKNOWN_PORT;
    }
    
    /** 
     * Returns the name of server that will be replicating this server's data 
     */
    public String getReplicatingServer(String serverName)
    {
        HierarchicalConfiguration serverConfig = 
            getServerConfiguration(serverName);
            
        return serverConfig != null ? 
            serverConfig.getString(REPLICATING_SERVER_ATTRIBUTE)
            : null;
    }
    
    /** 
     * Returns the name of server whose data will be replicated in the 
     * given server.
     */
    public String getIamReplicaFor(String serverName)
    {
        @SuppressWarnings("unchecked")
        List<HierarchicalConfiguration> serverConfigs = 
           (List<HierarchicalConfiguration>)
                myTopologyConfiguration.configurationsAt(SERVERS_GROUP_KEY);
        
        for (HierarchicalConfiguration serverConfig : serverConfigs)
        {
            String replicatingServerName = 
                serverConfig.getString(REPLICATING_SERVER_ATTRIBUTE);
            
            if (   replicatingServerName != null 
                && !replicatingServerName.isEmpty()
                && replicatingServerName.equalsIgnoreCase(serverName)) 
            {
                return serverConfig.getString(NAME_ATTRIBUTE);
            }
        }
        
        return null;
    }
}
