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

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.event.Event;
import org.hit.util.LogFactory;

/**
 * Defines the contract for an interface that can be used by consumers for
 * accessing events from the <code>EventPassingQueue</code>.
 *
 * @author Balraja Subbiah
 */
public class ConsumerAccess extends AbstractAccess
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(ConsumerAccess.class);

    private volatile int myConsumedIndex;

    private final EventPassingQueue myEPQ;
    
    private final AccessorID myAccessorID;
    
    private final Lock myLock;
    
    private final Condition myWaitCondition;

    /**
     * CTOR
     */
    public ConsumerAccess(AccessorID        accessorID,
                          EventPassingQueue ePQ)
    {
        super(accessorID.getWaitStrategy());
        myAccessorID    = accessorID;
        myEPQ           = ePQ;
        myConsumedIndex = -1;
        myLock          = new ReentrantLock();
        myWaitCondition = myLock.newCondition();

    }
    
    public AccessorID getAccessorID()
    {
        return myAccessorID;
    }

    /** Returns the <code>Event</code> for consumption */
    @Override
    public Event consume()
    {
        while (myConsumedIndex == myEPQ.getCursor()) {
            // Busy wait if the queue is empty
            waitFor();
        }
        myConsumedIndex = myEPQ.nextIndex(myConsumedIndex);
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("The " + myAccessorID + " is consuming message "
                     + " at " + myConsumedIndex);
        }
        return myEPQ.eventAt(myConsumedIndex);
    }

    /**
     * Returns the value of consumedIndex
     */
    public int getConsumedIndex()
    {
        return myConsumedIndex;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Event event)
    {
        throw new RuntimeException("Illegal access");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void waitFor()
    {
        if (getWaitStrategy() == WaitStrategy.CONDITIONAL_WAIT) {
            try {
                myLock.lock();
                myWaitCondition.await();
             }
             catch (InterruptedException e) {
                 // Ignore.
             }
             finally {
                 myLock.unlock();
             }
        }
        else {
            super.waitFor();
        }
    }
    
    /**
     * Notifies the consumer that an {@link Event} has been published to the
     * {@link EventPassingQueue} from which it's consuming the events.
     */
    public void newEventArrived()
    {
        if (getWaitStrategy() == WaitStrategy.CONDITIONAL_WAIT) {
            myLock.lock();
            myWaitCondition.signalAll();
            myLock.unlock();
        }
    }
}
