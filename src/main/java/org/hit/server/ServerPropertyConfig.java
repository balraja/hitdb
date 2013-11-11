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
 * Implements {@link ServerConfig} by extracting out the values from property.
 * 
 * @author Balraja Subbiah
 */
public class ServerPropertyConfig implements ServerConfig
{
    public static final String SERVER_NAME_PROPERTY = "org.hit.server.name";
    
    public static final String SERVER_REPLICATION_PROPERTY = "org.hit.server.replication";
    
    public static final String SERVER_COUNT_PROPERTY = "org.hit.server.count";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getServerName()
    {
        return System.getProperty(SERVER_NAME_PROPERTY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getInitialServerCount()
    {
        return Integer.parseInt(System.getProperty(SERVER_COUNT_PROPERTY));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getReplicationFactor()
    {
        return Integer.parseInt(System.getProperty(SERVER_REPLICATION_PROPERTY));
    }

}
