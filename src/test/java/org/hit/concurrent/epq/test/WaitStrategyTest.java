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

import static org.junit.Assert.*;

import org.hit.concurrent.epq.EventPassingQueue;
import org.hit.concurrent.epq.WaitStrategy;
import org.junit.Test;

/**
 * An unit test for verifying whether {@link WaitStrategy} used when sending
 * messages via {@link EventPassingQueue} is working fine.
 * 
 * @author Balraja Subbiah
 */
public class WaitStrategyTest
{
    
    @Test
    public void test()
    {
        EventPassingQueue epq = new EventPassingQueue(20);
        
        Thread publisherThread = 
            new Thread(new Publisher(epq,
                                     new TestAccessorID(
                                         "publisher",
                                         WaitStrategy.SLEEP), 
                                     10));
        Consumer consumer = 
                new Consumer(epq,
                             new TestAccessorID(
                                 "consumer",
                                  WaitStrategy.CONDITIONAL_WAIT));
        
        Thread consumerThread = new Thread(consumer);
        
        publisherThread.start();
        consumerThread.start();
        try {
            publisherThread.join();
            consumerThread.join();
            assertEquals(consumer.getReceivedEvents().last().getEventID(),
                         10);
        }
        catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }
    
}
