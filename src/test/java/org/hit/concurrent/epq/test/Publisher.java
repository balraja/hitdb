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

import org.hit.concurrent.epq.AccessorID;
import org.hit.concurrent.epq.EventPassingQueue;

/**
 * Defines the contract for a test publisher into the {@link EventPassingQueue}.
 * 
 * @author Balraja Subbiah
 */
public class Publisher implements Runnable
{
    private final EventPassingQueue myTestEPQ;
    
    private final AccessorID        myPublisherID;
    
    private final int               myMaxEvents;

    /**
     * CTOR
     */
    public Publisher(EventPassingQueue testEPQ, 
                     AccessorID        publisherID,
                     int               maxEvents)
    {
        super();
        myTestEPQ     = testEPQ;
        myPublisherID = publisherID;
        myMaxEvents   = maxEvents;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run()
    {
        for (int i = 1; i <= myMaxEvents ; i++) {
            myTestEPQ.publish(myPublisherID, new TestEvent(i));
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
            }
        }
        myTestEPQ.publish(myPublisherID, new StopEvent());
    }
}
