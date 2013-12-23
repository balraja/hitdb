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

package org.hit.communicator.nio;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.communicator.Communicator;
import org.hit.communicator.CommunicatorException;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.NodeID;
import org.hit.communicator.SerializerFactory;
import org.hit.concurrent.CloseableLock;
import org.hit.util.LogFactory;
import org.hit.util.NamedThreadFactory;

import com.google.inject.Inject;

/**
 * Implements <code>Communicator</code> using NIO's nonblocking channels. By
 * default it binds to the port 17000 and starts listening for incoming
 * connections.
 *
 * @author Balraja Subbiah
 */
public class NIOCommunicator implements Communicator
{
    /**
     * A simple task to take care of selecting keys from the selector and
     * scheduling tasks for further work.
     */
    private class SelectTask implements Runnable
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            try {
                LOG.info("Selector started");
                while (!myShouldStop.get()) {
                    try (CloseableLock lock =
                                    mySessionMapLock.open())
                    {
                        for (Session session : myIdSessionMap.values()) {
                            session.write();
                        }
                    }
                    int n = mySelector.select(1000);
                    if (n == 0) {
                        continue;
                    }

                    Set<SelectionKey> selectedKeys =
                        mySelector.selectedKeys();
                    Iterator<SelectionKey>
                        keyIterator = selectedKeys.iterator();

                    while(keyIterator.hasNext()) {

                        SelectionKey sKey = keyIterator.next();

                        if (!sKey.isValid()) {
                            continue;
                        }

                        if (sKey.isAcceptable()) {

                            ServerSocketChannel serverSocketChannel =
                                (ServerSocketChannel) sKey.channel();

                            SocketChannel channel =
                                serverSocketChannel.accept();

                            if (channel == null) {
                                continue;
                            }

                            if (LOG.isLoggable(Level.FINE)) {
                                LOG.fine("Acceping connection from "
                                         + channel.getRemoteAddress());
                            }

                            channel.configureBlocking(false);
                            SelectionKey key =
                                channel.register(mySelector,
                                                 SelectionKey.OP_READ);

                            Session session =
                                new Session(channel,
                                            mySerializerFactory.makeSerializer());

                            try (CloseableLock lock =
                                     mySessionMapLock.open())
                            {
                                myKeySessionMap.put(key, session);
                            }
                        }
                        else if (sKey.isConnectable()) {

                            Session session = myKeySessionMap.get(sKey);
                            if (session != null) {
                                session.connected();
                            }
                        }
                        else if (sKey.isReadable()) {
                            if (LOG.isLoggable(Level.FINE)) {
                                LOG.fine("The channel "
                                         + sKey.channel() + " is readable");
                            }
                            Session session = myKeySessionMap.get(sKey);
                            if (session != null) {
                                Message message = session.readMessage();
                                try (CloseableLock lock =
                                         mySessionMapLock.open())
                                {
                                    myIdSessionMap.put(message.getSenderId(),
                                                       session);
                                }
                                for (MessageHandler handler : myHandlers)
                                {
                                    handler.handle(message);
                                }
                            }
                        }
                        // It's very important to remove the keys
                        // after processing. Otherwise channel
                        // will not be selected next time.
                        keyIterator.remove();
                    }
                }
            }
            catch (Throwable e)
            {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }
    }

    private static final Logger LOG = LogFactory.getInstance().getLogger(
            NIOCommunicator.class);

    private final Collection<MessageHandler> myHandlers;

    private final IPNodeID myId;

    private final Map<NodeID, Session> myIdSessionMap;

    private final Map<SelectionKey, Session> myKeySessionMap;

    private final ExecutorService mySelectableExecutor;

    private final Selector mySelector;

    private final SerializerFactory mySerializerFactory;

    private final ServerSocketChannel myServerSocketChannel;

    private final CloseableLock mySessionMapLock;

    private final AtomicBoolean myShouldStop;

    /** CTOR */
    @Inject
    public NIOCommunicator(SerializerFactory  factory,
                           NodeID             nodeID)
    {
        try {
            myServerSocketChannel = ServerSocketChannel.open();
            mySelector = Selector.open();
            mySerializerFactory = factory;
            myHandlers = new ArrayList<>();
            myId       = (IPNodeID) nodeID;
            myIdSessionMap = new HashMap<NodeID, Session>();
            mySelectableExecutor =
                Executors.newSingleThreadExecutor(
                    new NamedThreadFactory(NIOCommunicator.class, true));
            myKeySessionMap = new ConcurrentHashMap<>();
            myShouldStop = new AtomicBoolean(false);
            mySessionMapLock = new CloseableLock(new ReentrantLock());
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addMessageHandler(MessageHandler handler)
    {
        myHandlers.add(handler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendTo(NodeID node, Message m) throws CommunicatorException
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Sending message " + m + " to node"  + node);
        }
        try (CloseableLock lock = mySessionMapLock.open()) {
            Session session = myIdSessionMap.get(node);
            if (session == null) {
                SocketChannel socketChannel =
                    SocketChannel.open(((IPNodeID) node).getIPAddress());
                socketChannel.configureBlocking(false);
                SelectionKey key =
                    socketChannel.register(mySelector,
                                           SelectionKey.OP_READ
                                           | SelectionKey.OP_CONNECT);
                session =
                    new Session(socketChannel,
                                mySerializerFactory.makeSerializer());
                myKeySessionMap.put(key, session);
                myIdSessionMap.put(node, session);
                session.cacheForWrite(m);
            }
            else {
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Adding message to the cache");
                }
                session.cacheForWrite(m);
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Successfully added message to the cache");
                }
            }
        }
        catch(IOException e) {
            throw new CommunicatorException(e);
        }
    }

    /** Starts the communicator  */
    @Override
    public void start() throws CommunicatorException
    {
        LOG.info("Starting NIO communicator");
        try {
            ServerSocket serverSocket = myServerSocketChannel.socket();
            serverSocket.bind(myId.getIPAddress());
            myServerSocketChannel.configureBlocking(false);
            myServerSocketChannel.register(mySelector,
                                           SelectionKey.OP_ACCEPT);
            LOG.info("Bound server to the address "
                     + myId.getIPAddress());
            mySelectableExecutor.execute(new SelectTask());
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new CommunicatorException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        LOG.info("Stopping NIO communicator bound to " +
                 myId.getIPAddress());
        try {
            myShouldStop.set(true);
            mySelectableExecutor.shutdownNow();
            mySelector.close();
            myServerSocketChannel.close();
            for (Session session : myIdSessionMap.values()) {
                session.close();
            }
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
