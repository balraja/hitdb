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

import java.util.Map.Entry;
import java.util.TreeMap;

import org.hit.communicator.NodeID;
import org.hit.key.Keyspace;

/**
 * Extends <code>PartitionTable</code> to support lookup on a linear fashion.
 * 
 * @author Balraja Subbiah
 */
public class LinearTable<S extends Comparable<S>, 
                         T extends Comparable<T>> 
    extends PartitionTable<S,T>
{
    /**
     * CTOR
     */
    public LinearTable(String tableName, Keyspace<S,T> keyspace)
    {
        super(tableName, keyspace);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected NodeID doLookup(T                  key,
                              TreeMap<T, NodeID> nodeMap)
    {
        Entry<T, NodeID> maxEntry = nodeMap.ceilingEntry(key);
        return maxEntry != null ? maxEntry.getValue() : null;
    }
}
