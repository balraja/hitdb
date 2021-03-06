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

package org.hit.actors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.event.Event;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

/**
 * Defines the contract for an actor in the system. The various parts of the
 * system is organized into actors which interact using <code>EventBus
 * </code>. An actor as such is a state machine which responds to the various
 * events published by the different parts of system. It communicates with
 * other actors by sending events to them.
 *
 * @author Balraja Subbiah
 */
public abstract class Actor
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(Actor.class);
            
    private static final int DEFAULT_EVENT_QUEUE_SIZE = 128;

    private final ExecutorService myActorExecutor;

    private final ActorID myActorID;

    private final EventBus myEventBus;

    private final AtomicBoolean myShouldStop;

    /**
     * CTOR
     */
    public Actor(EventBus eventBus, ActorID id)
    {
        myActorID = id;
        myEventBus = eventBus;
        myShouldStop = new AtomicBoolean(false);
        myActorExecutor =
            Executors.newFixedThreadPool(
                1,
                new NamedThreadFactory(id.name()));
    }

    /**
     * Returns the value of actorID
     */
    public ActorID getActorID()
    {
        return myActorID;
    }

    /**
     * Returns the value of eventBus
     */
    public EventBus getEventBus()
    {
        return myEventBus;
    }

    /**
     * Subclasses should override this method for implementing the event loop.
     */
    protected abstract void processEvent(Event event);

    /**
     * Registers the events to be received via event bus.
     */
    protected abstract void registerEvents();

    /**
     * Starts the component.
     */
    public void start()
    {
        myEventBus.register(myActorID, DEFAULT_EVENT_QUEUE_SIZE);
        registerEvents();
        myActorExecutor.submit(new Runnable() {
            @Override
            public void run() {
                while (!myShouldStop.get()) {
                    Event event = myEventBus.consume(myActorID);
                    try {
                        processEvent(event);
                    }
                    catch (Throwable e) {
                        LOG.log(Level.SEVERE, 
                                "Received exception while processing " 
                                + event + " for actor " + myActorID,
                                e);
                    }
                }
            }});
        
        LOG.info("Actor " + myActorID + " has been started successfully");
        
    }

    /**
     * Stops the actor from processing the events.
     */
    public void stop()
    {
        myShouldStop.set(true);
        myActorExecutor.shutdownNow();
    }
    
    /** A helper method to publish the event to the event bus */
    public void publish(Event event)
    {
        myEventBus.publish(myActorID, event);
    }
}
