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
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.communicator.Communicator;
import org.hit.communicator.Message;
import org.hit.communicator.MessageHandler;
import org.hit.communicator.MessageSerializer;
import org.hit.communicator.NodeID;
import org.hit.util.LogFactory;

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

    private final Map<InetSocketAddress, List<ByteBuffer>> myAddressToData;
    
    private final IPNodeID myId;

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
            myAddressToData = new ConcurrentHashMap<>();
            myId            = (IPNodeID) nodeID;

            ServerSocket serverSocket = myServerSocketChannel.socket();
            serverSocket.bind(myId.getIPAddress());
            myServerSocketChannel.configureBlocking(false);
            myServerSocketChannel.register(mySelector, SelectionKey.OP_ACCEPT);
        }
        catch (IOException e) {
            LOG.log(Level.INFO, e.getMessage(), e);
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
        
        socketChannel.close();
    }

    private void registerChannel(SocketChannel channel, int key,
            Object attachment) throws IOException
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
            IPNodeID targetNode = (IPNodeID) node;
            List<ByteBuffer> data =
                myAddressToData.get(targetNode.getIPAddress());
            if (data == null) {
                data = new CopyOnWriteArrayList<>();
                myAddressToData.put(targetNode.getIPAddress(), data);
            }
            data.add(mySerializer.serialize(m));
            SocketChannel writableChannel = SocketChannel.open();
            writableChannel.configureBlocking(false);
            SelectionKey key =
                writableChannel.register(mySelector,
                                         SelectionKey.OP_CONNECT);
            key.attach(targetNode.getIPAddress());
            writableChannel.connect(targetNode.getIPAddress());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Starts the communicator */
    public void start()
    {
        try {
            while (true) {
                int n = mySelector.select();
                if (n == 0) {
                    continue;
                }
                else {
                    for (SelectionKey sKey : mySelector.selectedKeys()) {
                        if (sKey.isAcceptable()) {
                            ServerSocketChannel serverSocketChannel =
                                (ServerSocketChannel) sKey.channel();
                            SocketChannel channel =
                                serverSocketChannel.accept();

                            registerChannel(channel,
                                            SelectionKey.OP_READ
                                            | SelectionKey.OP_WRITE,
                                            null);
                        }
                        else if (sKey.isReadable()) {
                            readData((SocketChannel) sKey.channel());
                        }
                        else if (sKey.isWritable()) {
                            SocketChannel socketChannel =
                                (SocketChannel) sKey.channel();
                            
                            @SuppressWarnings("unchecked")
                            List<ByteBuffer> writableBuffers =
                                (List<ByteBuffer>) sKey.attachment();
                            writeData(socketChannel, writableBuffers);
                        }
                        else if (sKey.isConnectable()) {
                            SocketChannel socketChannel =
                                (SocketChannel) sKey.channel();
                            List<ByteBuffer> writableBuffers =
                                myAddressToData.get(sKey.attachment());
                            
                            if (writableBuffers != null) {
                                registerChannel(socketChannel,
                                                SelectionKey.OP_WRITE,
                                                writableBuffers);
                            }
                        }
                    }
                }
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** Reads data from the <code>SocketChannel</code> */
    private void writeData(SocketChannel    socketChannel,
                           List<ByteBuffer> writableBuffers) throws IOException
    {
        for (ByteBuffer writableBuffer : writableBuffers) {
            socketChannel.write(writableBuffer);
        }
        socketChannel.close();
    }
}
