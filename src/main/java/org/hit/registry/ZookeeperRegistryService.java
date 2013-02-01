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

package org.hit.registry;

import java.util.List;

import org.hit.communicator.NodeID;
import org.hit.db.model.Schema;
import org.hit.distribution.KeyPartitioner;
import org.hit.zookeeper.ZooKeeperClient;

import com.google.inject.Inject;

/**
 * An implementation of <code>RegistryService</code> that extracts the 
 * necessary information stored under the zookeeper.
 * 
 * @author Balraja Subbiah
 */
public class ZookeeperRegistryService implements RegistryService
{
    private final ZooKeeperClient myZooKeeperClient;

    /**
     * CTOR
     */
    @Inject
    public ZookeeperRegistryService(ZooKeeperClient zooKeeperClient)
    {
        myZooKeeperClient = zooKeeperClient;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>> KeyPartitioner<K>
        getKeyPartitioner(String tableName)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Schema getSchema(String tableName)
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeID> getServerNodes()
    {
        return myZooKeeperClient.getServerIDS();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isUp()
    {
        return myZooKeeperClient.isReady();
    }
}
