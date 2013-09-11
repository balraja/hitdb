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

package org.hit.communicator.test;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hit.communicator.Communicator;
import org.hit.communicator.CommunicatorException;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.NodeID;
import org.hit.communicator.ObjectStreamSerializerFactory;
import org.hit.communicator.nio.IPNodeID;
import org.hit.communicator.nio.NIOCommunicator;
import org.hit.db.model.mutations.BatchMutation;
import org.hit.example.Airport;
import org.hit.example.AirportDataLoader;
import org.hit.messages.DBOperationMessage;
import org.junit.Test;

/**
 * Defines the testcases for {@link NIOCommunicator}.
 *
 * @author Balraja Subbiah
 */
public class NIOCommunicatorTest
{
    /**
     * Extends <code>Runnable</code> to support performing receiver abstraction
     */
    private static class Receiver implements Runnable, MessageHandler
    {
        private final Communicator myCommunicator;

        private Message myReceivedMessage;

        private final AtomicBoolean myResultAvailable;

        private long myStartTime;

        /**
         * CTOR
         */
        public Receiver(NodeID receiverID)
        {
            myCommunicator =
                new NIOCommunicator(new ObjectStreamSerializerFactory(),
                                    receiverID);

            myResultAvailable = new AtomicBoolean(false);
            myReceivedMessage = null;
        }

        /**
         * Returns the value of receivedMessage
         */
        public Message getReceivedMessage()
        {
            return myReceivedMessage;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void handle(Message message)
        {
            myReceivedMessage = message;
            System.out.println("Received message from "
                               + myReceivedMessage.getNodeId());
            myResultAvailable.set(true);
            myCommunicator.stop();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            myCommunicator.addMessageHandler(this);
            try {
                myCommunicator.start();
            }
            catch (CommunicatorException e1) {
                e1.printStackTrace();
            }
            myStartTime = System.currentTimeMillis();
            System.out.println("Started the receiving agent's communicator");
            while ((System.currentTimeMillis() - myStartTime) < (30 * 1000)
                   && !myResultAvailable.get())
            {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                }
            }
        }
    }

    /**
     * Extends <code>Runnable</code> to support performing sender abstraction
     */
    private static class Sender implements Runnable
    {
        private final Communicator myCommunicator;

        private final Message myMessage;

        private final NodeID myReceiverID;

        private final NodeID mySenderID;

        private final AtomicBoolean myStop;

        /**
         * CTOR
         */
        public Sender(NodeID senderId, NodeID receiverID, Message message)
        {
            super();
            mySenderID = senderId;
            myReceiverID = receiverID;
            myMessage = message;
            myCommunicator =
                new NIOCommunicator(new ObjectStreamSerializerFactory(),
                                    mySenderID);
            myStop = new AtomicBoolean(false);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            try {
                myCommunicator.start();
                System.out.println("Started the sending agent's communicator");
                myCommunicator.sendTo(myReceiverID, myMessage);
            }
            catch (CommunicatorException e1) {
                e1.printStackTrace();
            }
            System.out.println("Sent message to the " + myReceiverID);
            while (!myStop.get()) {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                }
            }
            myCommunicator.stop();
        }

        public void stop()
        {
            myStop.set(true);
        }
    }

    @Test
    public void test()
    {
        NodeID senderID = new IPNodeID(25000);
        NodeID receiverID = new IPNodeID(25001);
        List<Airport> airportList = new AirportDataLoader().loadTestData();
        BatchMutation<Long, Airport> batchMutation =
            new BatchMutation<>("Airport", new ArrayList<>(airportList.subList(0, 5)));
        DBOperationMessage message =
            new DBOperationMessage(senderID, 1L, batchMutation);

        Sender sender = new Sender(senderID, receiverID, message);
        Thread senderThread = new Thread(sender);

        Receiver receiver = new Receiver(receiverID);
        Thread receiverThread = new Thread(receiver);

        receiverThread.start();
        try {
            Thread.sleep(2000);
        }
        catch (InterruptedException e1) {
        }

        senderThread.start();

        try {
            receiverThread.join();
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        sender.stop();

        try {
            senderThread.join();
        }
        catch (InterruptedException e) {
        }
        assertNotNull(receiver.getReceivedMessage());
    }
}
