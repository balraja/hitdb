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

/**
 * Defines an interface for the server configuration.
 * 
 * @author Balraja Subbiah
 */
public interface ServerConfig
{
    /**
     * The name of this server.
     */
    public String getServerName();
    
    /**
     * Returns the number of servers that get's started initially.
     */
    public int getInitialServerCount();
    
    /**
     * Returns the number of servers to which current server's data
     * has to be replicated.
     */
    public int getReplicationFactor();
    
    /**
     * Returns the name of server whose data is replicated by this
     * server.
     */
    public String getReplicationGroup();
    
    /**
     * Returns true if the server is marked as master initially.
     */
    public boolean isMaster();
    
    /**
     * Returns the interval in seconds during which the heartbeats are to be
     * published.
     */
    public int getHeartBeatIntervalSecs();
    
    /**
     * Returns the interval in seconds during which gossip updates are to
     * be published.
     */
    public int getGossipUpdateSecs();
}
