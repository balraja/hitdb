/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

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

package org.hit.concurrent.epq;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.hit.event.Event;

/**
 * Defines the contract for an interface that can be used for publishing the
 * events to <code>EventPassingQueue</code>.
 * 
 * @author Balraja Subbiah
 */
public class PublisherAccess extends AbstractAccess
{
    private final EventPassingQueue myEPQ;
    
    private final Collection<ConsumerAccess> myConsumers;
    
    /**
     * CTOR
     */
    public PublisherAccess(WaitStrategy waitStrategy, EventPassingQueue ePQ)
    {
        super(waitStrategy);
        myEPQ = ePQ;
        myConsumers =
            Collections.synchronizedList(new ArrayList<ConsumerAccess>());
    }
    
    /**
     * Adds <code>ConsumerAcess</code> to be tracked by this <code>Producer
     * </code>.
     */
    public void addConsumer(ConsumerAccess consumer)
    {
        myConsumers.add(consumer);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Event consume()
    {
        throw new RuntimeException("Illegal access as consumer");
    }

    /** Publishes <code>Event</code>s to the <code>EventPassingQueue</code> */
    @Override
    public void publish(Event event)
    {
        // Busy waits till all the consumers have consumed upto
        // the current item in the queue.
        boolean canPublish = true;
        int cursor = myEPQ.getCursor();
        do {
            for (ConsumerAccess consumerAccess : myConsumers) {
                if (consumerAccess.getConsumedIndex() != cursor) {
                    canPublish = false;
                }
            }
        }
        while(!canPublish);
        
        while(!myEPQ.publish(event, myEPQ.nextIndex(cursor))) {
            cursor = myEPQ.getCursor();
        }
    }
}
