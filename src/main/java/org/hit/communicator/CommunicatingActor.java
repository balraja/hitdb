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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.actors.Actor;
import org.hit.actors.ActorID;
import org.hit.actors.EventBus;
import org.hit.event.Event;
import org.hit.event.SendMessageEvent;
import org.hit.pool.PooledObjects;
import org.hit.util.LogFactory;

import com.google.inject.Inject;

/**
 * Implements <code>Actor</code> to support implementing an agent for
 * communicating with the outside world.
 *
 * @author Balraja Subbiah
 */
public class CommunicatingActor extends Actor
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(CommunicatingActor.class);

    private final Communicator myCommunicator;


    /**
     * CTOR
     */
    @Inject
    public CommunicatingActor(EventBus eventBus, Communicator communicator)
    {
        super(eventBus, ActorID.COMMUNICATOR);
        myCommunicator = communicator;
        myCommunicator.addMessageHandler(new MessageHandler() {
            @Override
            public void handle(Message message)
            {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Received " + message + " from "
                             + message.getSenderId());
                }
                LOG.info("Received " + message + " from "
                        + message.getSenderId());
                publish(message);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Returned from publishing the message");
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
                try {
                    myCommunicator.sendTo(nodeID, sme.getMessage());
                    PooledObjects.freeInstance(sme);
                }
                catch (CommunicatorException e) {
                    LOG.log(Level.SEVERE,
                            "Sending message "
                             + sme.getMessage().getClass().getSimpleName()
                             + " to " + nodeID + " failed",
                             e);
                }
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        super.start();
        try {
            myCommunicator.start();
        }
        catch (CommunicatorException e) {
           throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        super.stop();
        myCommunicator.stop();
    }
}
