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
package org.hit.concurrent.epq.test;

import java.util.SortedSet;
import java.util.TreeSet;

import org.hit.concurrent.epq.AccessorID;
import org.hit.concurrent.epq.EventPassingQueue;
import org.hit.event.Event;

/**
 * Defines the contract for a consumer that consumes event from the given
 * {@link EventPassingQueue}.
 * 
 * @author Balraja Subbiah
 */
public class Consumer implements Runnable
{
    private final EventPassingQueue    myEventPassingQueue;
    
    private final AccessorID           myAccessorID;
    
    private final SortedSet<TestEvent> myReceivedEvents;

    /**
     * CTOR
     */
    public Consumer(EventPassingQueue eventPassingQueue, AccessorID accessorID)
    {
        super();
        myEventPassingQueue = eventPassingQueue;
        myAccessorID = accessorID;
        myReceivedEvents = new TreeSet<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run()
    {
        while (true) {
            Event e = myEventPassingQueue.consume(myAccessorID);
            if (e instanceof StopEvent) {
                break;
            }
            else if (e instanceof TestEvent) {
                myReceivedEvents.add((TestEvent) e);
            }
        }
    }

    /**
     * Returns the value of receivedEvents
     */
    public SortedSet<TestEvent> getReceivedEvents()
    {
        return myReceivedEvents;
    }
}
