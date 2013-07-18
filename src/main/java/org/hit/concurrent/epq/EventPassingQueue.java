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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.hit.concurrent.CloseableLock;
import org.hit.event.Event;

/**
 * This class is responsible for buffering data passed between two
 * components of a system. This is inspired by the ideas published by
 * the disruptor system. This is suitable for one publisher and one
 * consumer scenario.
 *
 * At it's heart is a circular buffer on which producer claims a slot,
 * waits for the consumer to consume the data in that slot and then
 * overrides the data in that slot.
 *
 * <pre>
 * final MessagePassingQueue queue = new MessagePassingQueue();
 *
 * new Runnable() {
 *     public void run() {
 *         Message m = generate_message
 *         queue.publish(m);
 *     }
 * }
 *
 * new Runnable() {
 *     public void run() {
 *         Message m = queue.consume(consumedIndex);
 *     }
 * }
 * </pre>
 *
 * @author Balraja Subbiah
 */
public class EventPassingQueue
{
    private final Event[] myBuffer;

    private final Map<AccessorID, ConsumerAccess> myConsumers;

    private final AtomicInteger myCursor;

    private final CloseableLock myLock;

    private final Map<AccessorID, PublisherAccess> myPublishers;

    private final int mySize;

    private final WaitStrategy myWaitStrategy;

    /**
     * CTOR
     */
    public EventPassingQueue(int size, WaitStrategy waitStrategy)
    {
        assert(size % 2 == 0);
        mySize = size;
        myBuffer = new Event[size];
        myCursor = new AtomicInteger(-1);
        myPublishers = new HashMap<>();
        myConsumers = new HashMap<>();
        myWaitStrategy = waitStrategy;
        myLock = new CloseableLock(new ReentrantLock());
    }

    /** Consumes <code>Event</code> from the queue */
    public Event consume(AccessorID consumerID)
    {
        ConsumerAccess access = myConsumers.get(consumerID);
        if (access == null) {
            try (CloseableLock lock = myLock.open()) {
                access = new ConsumerAccess(myWaitStrategy, this);
                for (PublisherAccess publisher : myPublishers.values()) {
                    publisher.addConsumer(access);
                }
                myConsumers.put(consumerID, access);
            }
        }
        return access.consume();
    }

    /** Returns the <code>Message</code> to be consumed */
    Event eventAt(int index)
    {
        Event m = myBuffer[index];
        return m;
    }

    /**
     * Returns the value of cursor
     */
    public int getCursor()
    {
        return myCursor.get();
    }

    /**
     * Returns the size of quque.
     */
    public int getSize()
    {
        return mySize;
    }

    /**
     *  Returns the next index corresponding to the current value in the buffer
     */
    int nextIndex(int currIndex)
    {
        return ((currIndex + 1) % mySize);
    }

    /** Consumes <code>Event</code> from the queue */
    public void publish(AccessorID accessorID, Event event)
    {
        PublisherAccess access = myPublishers.get(accessorID);
        if (access == null) {
            try (CloseableLock lock = myLock.open()) {
                access = new PublisherAccess(myWaitStrategy, this);
                for (ConsumerAccess consumer : myConsumers.values()) {
                    access.addConsumer(consumer);
                }
                myPublishers.put(accessorID, access);
            }
        }
        access.publish(event);
    }

    /** Publishes the message to the specified index */
    public boolean publish(Event m, int currIndex, int publishedIndex)
    {
        // Producer claims the slot using compareAndSet operation.
        if (myCursor.compareAndSet(currIndex, publishedIndex)) {
            myBuffer[publishedIndex] = m;
            return true;
        }
        else {
            return false;
        }
    }
}