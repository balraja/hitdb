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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.event.Event;
import org.hit.util.LogFactory;

/**
 * Defines the contract for an interface that can be used for publishing the
 * events to <code>EventPassingQueue</code>.
 *
 * @author Balraja Subbiah
 */
public class PublisherAccess extends AbstractAccess
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(PublisherAccess.class);
    
    private final Collection<ConsumerAccess> myConsumers;

    private final EventPassingQueue myEPQ;
    
    private final AccessorID myAccessorID;

    /**
     * CTOR
     */
    public PublisherAccess(
        WaitStrategy waitStrategy, 
        EventPassingQueue ePQ,
        AccessorID accessorID)
    {
        super(waitStrategy);
        myAccessorID = accessorID;
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

        while (true) {
            // If the queue is full and there are no consumers then wait.
            if (myConsumers.isEmpty()
                && (myEPQ.getCursor() == (myEPQ.getSize() - 1)))
            {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("The queue is full  " + myEPQ.getCursor()
                             + " and the current queue size "
                             + myEPQ.getSize()
                             + "and there are no consumers "
                             +  myConsumers.isEmpty());
                }
                waitFor();
                continue;
            }
            // Get the next slot to publish.
            int currIndex = myEPQ.getCursor();
            int publishIndex = myEPQ.nextIndex(myEPQ.getCursor());
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Next published index " + publishIndex
                         + " curr index " + currIndex);
            }
            boolean canPublish = true;
            for (ConsumerAccess consumer : myConsumers) {
                // if the to be published index is yet to consumed index
                // then wait.
                if (publishIndex == consumer.getConsumedIndex()
                    && myEPQ.getCursor() != -1)
                {
                    if (LOG.isLoggable(Level.FINEST)) {
                        LOG.finest("The consumed index for " 
                                 + consumer.getAccessorID() 
                                 + " is at " 
                                 + consumer.getConsumedIndex());
                    }
                    canPublish = false;
                    break;
                }
            }
            if (canPublish && myEPQ.publish(event, currIndex, publishIndex)) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest(" The " + myAccessorID + " has successfully "
                             + " published the message " 
                             + event.getClass().getSimpleName()
                             + " to the slot " + publishIndex); 
                }
                return;
            }
            else {
                waitFor();
            }
        }
    }
}
