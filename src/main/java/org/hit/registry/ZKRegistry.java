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

import com.google.inject.Inject;

import java.util.List;

import org.hit.communicator.NodeID;
import org.hit.gms.GroupID;
import org.hit.zookeeper.ZooKeeperClient;

/**
 * Implements the {@link RegistryService} on top of zookeeper.
 * 
 * @author Balraja Subbiah
 */
public class ZKRegistry implements RegistryService
{
    private final ZooKeeperClient myZKClient;
    
    private final GroupID    myElectorateID;

    /**
     * CTOR
     */
    @Inject
    public ZKRegistry(ZooKeeperClient zKClient, 
                      GroupID electorateID)
    {
        super();
        myZKClient = zKClient;
        myElectorateID = electorateID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<NodeID> getServerNodes()
    {
        return myZKClient.getNodes(myElectorateID.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NodeID getMasterNode()
    {
        return myZKClient.getLockHolder(myElectorateID.toString()).getFirst();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isUp()
    {
        return myZKClient.isUp();
    }
}
