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

import org.hit.event.Event;

/**
 * Defines the contract for an interface that can be used for accessing
 * <code>EventPassingQueue</code> as a producer or consumer
 * 
 * @author Balraja Subbiah
 */
public interface Access
{
    /** Consumes <code>Event</code> from the queue */
    public Event consume();
    
    /** Publishes <code>Event</code> to the system */
    public void publish(Event event);
    
    /**
     * Performs the wait as per the <code>WaitStrategy</code>
     * specified with the event passing quque.
     */
    public void waitFor();
}
