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
package org.hit.election;

import com.google.inject.Inject;

import java.util.HashMap;
import java.util.Map;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.event.Event;
import org.hit.zookeeper.ZooKeeperClient;

/**
 * Defines the contract for ElectionManager that helps in selecting a 
 * leader among a group of nodes.
 * 
 * @author Balraja Subbiah
 */
public class ElectionManager extends Actor
{
    private final Map<ElectorateID, Electorate> myElectorateMap;
    
    private final ZooKeeperClient myZKClient;
    
    /**
     * CTOR
     */
    @Inject
    public ElectionManager(EventBus eventBus, ZooKeeperClient zc)
    {
        super(eventBus, new ActorID(ElectionManager.class.getSimpleName()));
        myElectorateMap = new HashMap<>();
        myZKClient      = zc;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        // TODO Auto-generated method stub
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        
    }
}
