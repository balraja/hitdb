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
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.communicator.Communicator;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.MessageSerializer;
import org.hit.communicator.NodeID;
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
    private static final Logger LOG = LogFactory.getInstance().getLogger(
            NIOCommunicator.class);

    private static final int BUFFER_SIZE = 1024 * 1024;

    private final ServerSocketChannel myServerSocketChannel;

    private final Selector mySelector;

    private final MessageSerializer mySerializer;

    private final Collection<MessageHandler> myHandlers;

    private final IPNodeID myId;
    
    private final ExecutorService myNIOExecutor;
    
    private final AtomicBoolean myShouldStop;

    /** CTOR */
    @Inject
    public NIOCommunicator(MessageSerializer  serializer,
                           NodeID             nodeID)
    {
        try {
            myServerSocketChannel = ServerSocketChannel.open();
            mySelector = Selector.open();
            mySerializer = serializer;
            myHandlers = new ArrayList<>();
            myId       = (IPNodeID) nodeID;
            myNIOExecutor =
                Executors.newSingleThreadExecutor(
                    new NamedThreadFactory(NIOCommunicator.class, true));
            myShouldStop = new AtomicBoolean(false);
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

    /** Reads data from the <code>SocketChannel</code> */
    private void readData(SocketChannel socketChannel) throws IOException
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);
        ByteBuffer messageBuffer = ByteBuffer.allocate(10 * BUFFER_SIZE);
        while (socketChannel.read(buffer) > 0) {
            buffer.flip();
            if (messageBuffer.remaining() == 0 && buffer.remaining() > 0) {
                messageBuffer = resize(messageBuffer);
            }
            messageBuffer.put(buffer);
            buffer.clear();
        }
        Message message = mySerializer.parse(messageBuffer);
        for (MessageHandler handler : myHandlers) {
            handler.handle(message);
        }
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Received the message " + message + " from "
                     + socketChannel.getRemoteAddress());
        }
        socketChannel.close();
    }

    private void registerChannel(
        SocketChannel channel, int key, Object attachment) throws IOException
    {
        channel.configureBlocking(false);
        SelectionKey sKey = channel.register(mySelector, key);
        if (attachment != null) {
            sKey.attach(attachment);
        }
    }

    private ByteBuffer resize(ByteBuffer buffer)
    {
        buffer.flip();
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
        newBuffer.put(buffer);
        return newBuffer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void sendTo(NodeID node, Message m)
    {
        try {
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Sending message " + m + " to node"  + node);
            }
            ByteBuffer serializedData = mySerializer.serialize(m);
            if (serializedData == null) {
                LOG.info("Unable to serialize the message");
                return;
            }
            IPNodeID targetNode = (IPNodeID) node;
            SocketChannel writableChannel = SocketChannel.open();
            writableChannel.configureBlocking(true);
            writableChannel.connect(targetNode.getIPAddress());
           
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Connection with " 
                         + writableChannel.getRemoteAddress()
                         + " is established ");
            }
            
            writableChannel.write(serializedData);
            
            if (LOG.isLoggable(Level.FINE)) {
                LOG.fine("Has written data to  " 
                          + writableChannel.getRemoteAddress());
            }
            writableChannel.close();
          
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /** Starts the communicator */
    public void start()
    {
        LOG.info("Starting NIO communicator");
        myNIOExecutor.execute(new Runnable() {
            public void run() {
                try {
                    ServerSocket serverSocket = myServerSocketChannel.socket();
                    serverSocket.bind(myId.getIPAddress());
                    myServerSocketChannel.configureBlocking(false);
                    myServerSocketChannel.register(mySelector, 
                                                   SelectionKey.OP_ACCEPT);
                    LOG.info("Bound server to the address " 
                             + myId.getIPAddress());
                    
                    while (!myShouldStop.get()) {
                        int n = mySelector.select();
                        if (n == 0) {
                            continue;
                        }
                        else {
                            for (SelectionKey sKey : mySelector.selectedKeys()) 
                            {
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
                                    registerChannel(channel,
                                                    SelectionKey.OP_READ
                                                    | SelectionKey.OP_WRITE,
                                                    null);
                                }
                                else if (sKey.isReadable()) {
                                    readData((SocketChannel) sKey.channel());
                                }
                            }
                        }
                    }
                }
                catch (IOException e) {
                    LOG.log(Level.SEVERE, e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
        });
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
            myNIOExecutor.shutdownNow();
            mySelector.close();
            myServerSocketChannel.close();
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }
}
