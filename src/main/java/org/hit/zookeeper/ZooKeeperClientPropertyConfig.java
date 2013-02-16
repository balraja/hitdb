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

package org.hit.zookeeper;

import org.hit.util.ApplicationProperties;

/**
 * @author Balraja Subbiah
 */
public class ZooKeeperClientPropertyConfig 
    implements ZooKeeperClientConfig
{
    private static final String ZOOKEEPER_HOSTS_PROPERTY = 
        "org.hit.zookeeper.hosts";
    
    private static final String ZOOKEEPER_TIMEOUT_PROPERTY = 
        "org.hit.zookeeper.timeout";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getHosts()
    {
        return ApplicationProperties.getProperty(ZOOKEEPER_HOSTS_PROPERTY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSessionTimeout()
    {
        return Integer.parseInt(
            ApplicationProperties.getProperty(ZOOKEEPER_TIMEOUT_PROPERTY));
    }
}
