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

package org.hit.transactions.workflow.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.hit.actors.EventBus;
import org.hit.event.Event;

/**
 * Extends {@link EventBus} to support recording events.
 * 
 * @author Balraja Subbiah
 */
public class RecordingEventBus extends EventBus
{
    private final Set<Class<? extends Event>> myInterestedEvents;
    
    private final EventNotificationListener myListener;

    /**
     * CTOR
     */
    public RecordingEventBus(Set<Class<? extends Event>> interestedEvents,
                             EventNotificationListener listener)
    {
        super();
        myInterestedEvents = interestedEvents;
        myListener = listener;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void publish(Event event)
    {
        if (myInterestedEvents.contains(event.getClass())) {
            myListener.handleEvent(event);
        }
    }
}
