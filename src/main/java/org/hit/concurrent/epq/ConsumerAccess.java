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

    /**
     * CTOR
     */
    public ConsumerAccess(AccessorID        accessorID,
                          WaitStrategy      waitStrategy,
                          EventPassingQueue ePQ)
    {
        super(waitStrategy);
        myAccessorID = accessorID;
        myEPQ = ePQ;
        myConsumedIndex = -1;
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
}
