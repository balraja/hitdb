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

package org.hit.communicator.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.buffer.BufferManager;
import org.hit.channel.ChannelInterface;
import org.hit.communicator.CommunicatorException;
import org.hit.communicator.Message;
import org.hit.communicator.MessageSerializer;
import org.hit.util.LogFactory;

/**
 * Defines the contract for a communication session open with other node.
 *
 * @author Balraja Subbiah
 */
public class Session
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(Session.class);

    private final Queue<Message> myBufferredMessages;

    private final Connection myConnection;

    private final MessageSerializer mySerializer;
    
    /**
     * CTOR
     */
    public Session(SocketChannel     socketChannel,
                   MessageSerializer serializer,
                   BufferManager      bufferManager)
    {
        myConnection = new Connection(socketChannel, bufferManager);
        mySerializer = serializer;
        myBufferredMessages = new ConcurrentLinkedQueue<>();
    }
    
    /**
     * CTOR
     */
    public Session(SocketChannel     socketChannel,
                   MessageSerializer serializer,
                   BufferManager     manager,
                   Message           message)
    {
        this(socketChannel, serializer, manager);
        myBufferredMessages.add(message);
    }

    /**
     * Adds to the write cache.
     */
    public void cacheForWrite(Message message)
    {
        myBufferredMessages.offer(message);
    }

    /** Closes the session with remote node */
    public void close()
    {
        try {
            myConnection.close();
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    /**
     * Marks the state as connected for this session.
     */
    public void connected() throws CommunicatorException
    {
        try {
            myConnection.getChannel().finishConnect();
            LOG.info("Connection extablished successfully with" +
                     " the remote host");
        }
        catch (IOException e) {
            throw new CommunicatorException(e);
        }
    }

    /**
     * Reads the <code>Message</code> published by the target node from the
     * underlying session.
     */
    public Collection<Message> readMessage() throws CommunicatorException
    {
        try {
            ChannelInterface ci = myConnection.read()
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Received bytes for reading " + readBuffer.remaining());
            }
            return mySerializer.parse(readBuffer);
        }
        catch (IOException e) {
            throw new CommunicatorException(e);
        }
    }
    
    public boolean hasMessagesToBeSent()
    {
        return !myBufferredMessages.isEmpty();
    }

    /**
     * Writes message to the target node when the channel is selected by
     * the <code>Selector</code>.
     */
    public void write() throws CommunicatorException
    {
        try {
            if (myBufferredMessages.isEmpty()) {
                return;
            }
            
            while (!myBufferredMessages.isEmpty()) {
                Message message = myBufferredMessages.poll();
                if (message != null) {
                    myConnection.send(mySerializer.serialize(message));
                }
                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine("Successfully sent the  " + message);
                }
            }
        }
        catch (IOException e) {
            throw new CommunicatorException(e);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return myConnection.toString();
    }
}
