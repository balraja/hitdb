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

package org.hit.actors.test;

import static org.junit.Assert.*;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.event.Event;
import org.junit.Test;

/**
 * Defines the testcase for {@link EventBus}.
 * 
 * @author Balraja Subbiah
 */
public class EventBusTest
{
    /** An {@link Actor} that acts as a provider of an event */
    private static class EventProvider extends Actor
    {
        /**
         * CTOR
         */
        public EventProvider(EventBus eventBus)
        {
            super(eventBus, new ActorID(EventProvider.class.getSimpleName()));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void processEvent(Event event)
        {
            
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void registerEvents()
        {
            
        }
    }
    
    /** An {@link Actor} that acts as a provider of an event */
    private static class EventConsumer extends Actor
    {
        private Event myReceivedEvent;
        
        /**
         * CTOR
         */
        public EventConsumer(EventBus eventBus, int index)
        {
            super(eventBus, 
                  new ActorID(EventConsumer.class.getSimpleName()
                              + " _ " + index));
        }
        
        /**
         * Returns the value of receivedEvent
         */
        public Event getReceivedEvent()
        {
            return myReceivedEvent;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void processEvent(Event event)
        {
            if (event instanceof DummyEvent) {
                myReceivedEvent = event;
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void registerEvents()
        {
            getEventBus().registerForEvent(DummyEvent.class, 
                                           getActorID());
        }
    }
    
    private static class ActorLauncher implements Runnable
    {
        private final Actor myActor;
        
        /**
         * CTOR
         */
        public ActorLauncher(Actor actor)
        {
            myActor = actor;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            myActor.start();
           
            if (myActor instanceof EventProvider) {
                try {
                    try {
                        Thread.sleep(200);
                    }
                    catch (InterruptedException e) {
                    }
                    myActor.getEventBus().publish(new DummyEvent());
                }
                catch (EventBusException e) {
                    e.printStackTrace();
                }
            }
            
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
            }
            myActor.stop();
        }
    }
    
    @Test
    public void test()
    {
        EventBus eventBus = new EventBus();
        Thread senderThread = 
            new Thread(new ActorLauncher(new EventProvider(eventBus)));
        
        EventConsumer consumer = new EventConsumer(eventBus, 1);
        Thread receiverThread = new Thread(new ActorLauncher(consumer));
        
        EventConsumer consumer1 = new EventConsumer(eventBus, 2);
        Thread receiverThread1 = new Thread(new ActorLauncher(consumer));
        
        receiverThread.start();
        senderThread.start();
        try {
            senderThread.join();
            receiverThread.join();
        }
        catch (InterruptedException e) {
        }
        
        assertNotNull(consumer.getReceivedEvent());
        assertNotNull(consumer.getReceivedEvent());
    }
}
