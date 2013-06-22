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
import org.hit.key.Partition;

/**
 * Defines the contract for the lookup service for accessing the
 * server related information on the client side.
 * 
 * @author Balraja Subbiah
 */
public interface RegistryService
{
    /**
     * Returns the table schema.
     */
    public <T extends Comparable<T>> Partition<T> getTablePartitioner(
        String tableName);
    
    /**
     * Returns the topology of hit server.
     */
    public List<NodeID> getServerNodes();
    
    /** Returns true if the registry service is up and running */
    public boolean isUp();
}
 