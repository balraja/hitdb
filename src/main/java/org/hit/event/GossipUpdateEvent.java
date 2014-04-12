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

package org.hit.event;

import java.util.ArrayList;
import java.util.Collection;

import org.hit.db.partitioner.Partitioner;
import org.hit.gossip.Gossip;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.PoolUtils;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines an event for updating the information to be spread via gossip
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(initialSize = 10, size = 100)
public class GossipUpdateEvent implements Event, Poolable
{
    private final Collection<Gossip> myGossip;
    
    /**
     * CTOR
     */
    public GossipUpdateEvent()
    {
        myGossip = new ArrayList<>();
    }

    /**
     * Factory method for creating an instance of <code>GossipUpdateEvent</code> 
     * and populating with various parameters.
     */
    public static GossipUpdateEvent create(Collection<Partitioner<?, ?>> collection)
    {
        GossipUpdateEvent event = 
            PooledObjects.getInstance(GossipUpdateEvent.class);
        event.myGossip.addAll(collection);
        return event;
    }

    /**
     * Returns the value of gossip
     */
    public Collection<Gossip> getGossip()
    {
        return myGossip;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        PoolUtils.free(myGossip);
        myGossip.clear();
    }
}
