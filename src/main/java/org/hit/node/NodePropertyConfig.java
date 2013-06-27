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

package org.hit.node;

import org.hit.util.ApplicationProperties;

/**
 * A default implementation of {@link NodeConfig} which reads the value 
 * from properties.
 * 
 * @author Balraja Subbiah
 */
public class NodePropertyConfig implements NodeConfig
{
    public static final String MASTER_PROPERTY = 
        "org.hit.node.isMaster";
    
    public static final String HEARTBEAT_PROPERTY =
        "org.hit.node.heartbeatInterval";
    
    private static final int DEFAULT_HB_INTERVAL = 5;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isMaster()
    {
        return Boolean.valueOf(
            ApplicationProperties.getProperty(MASTER_PROPERTY));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHeartBeatInterval()
    {
        String value = ApplicationProperties.getProperty(HEARTBEAT_PROPERTY);
        return value != null ? Integer.parseInt(value) : DEFAULT_HB_INTERVAL;
    }
}
