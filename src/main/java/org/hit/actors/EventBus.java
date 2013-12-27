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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.concurrent.epq.EventPassingQueue;
import org.hit.concurrent.epq.WaitStrategy;
import org.hit.event.Event;
import org.hit.util.LogFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * An event bus used for communication between various actors of the
 * system. Primarily an actor can register for an event. When an actor
 * owns an event, instances of that event type when published on the
 * event bus will be delivered to that actor. Alternatively we
 * can directly publish events to the actors. Each actor owns an
 * {@linkplain EventPassingQueue} and all the events delivered to that
 * actor will be published to the actor's queue.
 *
 * @author Balraja Subbiah
 */
public class EventBus
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(EventBus.class);

    private final Map<ActorID, EventPassingQueue> myActorToEPQ;

    private final Multimap<Class<? extends Event>, ActorID> myEvent2Actors;

    /**
     * CTOR
     */
    public EventBus()
    {
        myActorToEPQ = new ConcurrentHashMap<>();
        myEvent2Actors =
            Multimaps.<Class<? extends Event>, ActorID>synchronizedSetMultimap(
                HashMultimap.<Class<? extends Event>, ActorID>create());
    }

    /**
     * Consumes <code>Event</code>s delivered to this component.
     */
    public Event consume(ActorID actorID)
    {
        EventPassingQueue epq = myActorToEPQ.get(actorID);
        if (epq == null) {
            LOG.log(Level.SEVERE,
                    "Returning as the " + actorID + " hasn't "
                    + " registered itself with the event bus ");
            return null;
        }
        return epq.consume(actorID);
    }

    /**
     * Publishes the given <code>Event</code> to the actor.
     */
    private void publish(ActorID from, ActorID to, Event event)
    {
        EventPassingQueue epq = myActorToEPQ.get(to);
        if (epq == null) {
            LOG.log(Level.SEVERE,
                    "Returning as the actor " + to + " hasn't "
                    + " registered itself with the event bus ");
            return;
        }
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Publishing data into " + to + " 's queue");
        }
        epq.publish(from, event);
    }

    /**
     * Publishes the <code>Event</code> to interested actors.
     */
    public void publish(ActorID from, Event event) 
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Publishing event " + event.getClass().getSimpleName()
                     + " on " + toString());
        }
        Collection<ActorID> actors = myEvent2Actors.get(event.getClass());
        if (actors == null || actors.isEmpty()) {
            for (Map.Entry<Class<? extends Event>, Collection<ActorID>> entry : 
                     myEvent2Actors.asMap().entrySet())
            {
                if (entry.getKey().isAssignableFrom(event.getClass()))
                {
                    actors = entry.getValue();
                    break;
                }
            }
        }
        if (actors != null && !actors.isEmpty()) {
            for (ActorID to : actors) {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Publishing the event "
                             + event.getClass().getSimpleName()
                             + " to " + to);
                }
                publish(from, to, event);
            }
        }
        else {
            LOG.severe("No actor has registered to receive event of type " + 
                       event.getClass().getName());
        }
    }

    /**
     * Registers a component for receiving <code>Event</code>s sent to it.
     */
    public void register(ActorID actorID, int size)
    {
        if (!myActorToEPQ.containsKey(actorID)) {
            myActorToEPQ.put(actorID,
                             new EventPassingQueue(size,
                                                   WaitStrategy.SLEEP));
        }
    }

    /**
     * Registers a given component with the event bus to receive all events
     * of that type.
     */
    public void registerForEvent(Class<? extends Event> eventType,
                                 ActorID actorID)
    {
        myEvent2Actors.put(eventType, actorID);
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Following actors have registered for receiving the event "
                     + eventType.getName()
                     + " : "
                     + myEvent2Actors.get(eventType)
                     + " with event bus " + toString());
        }
    }
}
