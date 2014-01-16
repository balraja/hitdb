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
package org.hit.event;

import java.util.concurrent.TimeUnit;

import org.hit.actors.ActorID;

/**
 * An event to schedule a periodic task to be executed by the system.
 * 
 * @author Balraja Subbiah
 */
public class PeriodicTaskScheduleRequest implements Event
{
    private final ActorID myTargetActor;
    
    private final Runnable myRunnable;
    
    private final int myTime;
    
    private final TimeUnit myTimeUnit;

    /**
     * CTOR
     */
    public PeriodicTaskScheduleRequest(
        ActorID targetActor, Runnable runnable, int time, TimeUnit timeUnit)
    {
        super();
        myTargetActor = targetActor;
        myRunnable    = runnable;
        myTime        = time;
        myTimeUnit    = timeUnit;
    }

    /**
     * Returns the value of targetActor
     */
    public ActorID getTargetActor()
    {
        return myTargetActor;
    }

    /**
     * Returns the value of runnable
     */
    public Runnable getRunnable()
    {
        return myRunnable;
    }

    /**
     * Returns the value of time
     */
    public int getTime()
    {
        return myTime;
    }

    /**
     * Returns the value of timeUnit
     */
    public TimeUnit getTimeUnit()
    {
        return myTimeUnit;
    }
}
