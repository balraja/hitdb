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

package org.hit.communicator;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.actors.EventBusException;
import org.hit.event.Event;
import org.hit.event.SendMessageEvent;

import com.google.inject.Inject;

/**
 * Implements <code>Actor</code> to support implementing an agent for
 * communicating with the outside world.
 * 
 * @author Balraja Subbiah
 */
public class CommunicatingActor extends Actor
{
    private final Communicator myCommunicator;
    
    /**
     * CTOR
     */
    @Inject
    public CommunicatingActor(EventBus eventBus, Communicator communicator)
    {
        super(eventBus, new ActorID(CommunicatingActor.class.getSimpleName()));
        myCommunicator = communicator;
        myCommunicator.addMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message message)
            {
                try {
                    getEventBus().publish(message);
                }
                catch (EventBusException e) {
                    throw new RuntimeException(e);
                }
            }
        });;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void processEvent(Event event)
    {
        if (event instanceof SendMessageEvent) {
            SendMessageEvent sme = (SendMessageEvent) event;
            for (NodeID nodeID : sme.getTargets()) {
                myCommunicator.sendTo(nodeID, sme.getMessage());
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerEvents()
    {
        getEventBus().registerForEvent(SendMessageEvent.class, getActorID());
    }
}
