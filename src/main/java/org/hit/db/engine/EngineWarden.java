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

package org.hit.db.engine;

import java.util.Collection;

import org.hit.event.Event;

/**
 * Defines the contract for a type that's used for monitoring the
 * database engine.
 *
 * @author Balraja Subbiah
 */
public interface EngineWarden
{
    /** Handles the interested events */
    public void handleEvent(Event event);

    /** A helper method to get the interested events */
    public Collection<Class<? extends Event>> interestedEvents();

    /** A helper method to start the warden */
    public void start();

    /** Stops the warden */
    public void stop();
}
