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
package org.hit.time.keeper;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.event.Event;
import org.hit.event.PeriodicTaskNotification;
import org.hit.event.PeriodicTaskScheduleRequest;
import org.hit.util.NamedThreadFactory;

import com.google.inject.Inject;

/**
 * This actor is responsible for managing all periodic tasks to be executed
 * by different parts of the system.
 * 
 * @author Balraja Subbiah
 */
public class PeriodicTaskManager extends Actor
{
    private final ScheduledExecutorService myScheduler;
    
    /**
     * Implements {@link Runnable} to support publishing the task to the 
     * target actors at periodic interval as specified.
     */
    private class ScheduledTaskPublisher implements Runnable
    {
        private final ActorID myTargetActorID;
        
        private final Runnable myTask;

        /**
         * CTOR
         */
        public ScheduledTaskPublisher(ActorID targetActorID,
                                      Runnable task)
        {
            super();
            myTargetActorID = targetActorID;
            myTask          = task;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            getEventBus().publish(
                ActorID.TIME_KEEPER,
                myTargetActorID,
                new PeriodicTaskNotification(myTask));
        }
    }
    
    /**
     * CTOR
     */
    @Inject
    public PeriodicTaskManager(EventBus eventBus)
    {
        super(eventBus, ActorID.TIME_KEEPER);
        myScheduler = 
            Executors.newScheduledThreadPool(
                1, 
                new NamedThreadFactory(PeriodicTaskManager.class));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof PeriodicTaskScheduleRequest) {
            PeriodicTaskScheduleRequest scheduleRequest = 
                (PeriodicTaskScheduleRequest) event;
            
            myScheduler.scheduleAtFixedRate(
                new ScheduledTaskPublisher(
                    scheduleRequest.getTargetActor(),
                    scheduleRequest.getRunnable()),
                scheduleRequest.getTime(),
                scheduleRequest.getTime(),
                scheduleRequest.getTimeUnit());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(PeriodicTaskScheduleRequest.class,
                                       getActorID());
    }
    
}
