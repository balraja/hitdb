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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.util.LogFactory;

/**
 * An abstraction to capture the active connection between two machines.
 *
 * @author Balraja Subbiah
 */
public class Connection
{
    private static final int BUFFER_SIZE = 1024 * 1024;

    private static final Logger LOG =
        LogFactory.getInstance().getLogger(Connection.class);

    private final SocketChannel myChannel;

    /**
     * CTOR
     */
    public Connection(SocketChannel channel)
    {
        myChannel = channel;
    }

    /**
     * Closes the connection.
     */
    public void close() throws IOException
    {
        myChannel.close();
    }

    public SocketChannel getChannel()
    {
        return myChannel;
    }

    /**
     * Reads the value from tcp connection.
     */
    public ByteBuffer read() throws IOException
    {
        ByteBuffer messageBuffer =
            ByteBuffer.allocate(10 * BUFFER_SIZE);
        int readBytes = -1;
        ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

        while ((readBytes = myChannel.read(buffer)) > 0) {
            buffer.flip();

            if (messageBuffer.remaining() < readBytes) {
                messageBuffer = resize(messageBuffer);
            }
            messageBuffer.put(buffer.array(), 0, readBytes);
            buffer.clear();
        }
        return messageBuffer;
    }

    private ByteBuffer resize(ByteBuffer buffer)
    {
        buffer.flip();
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
        newBuffer.put(buffer);
        return newBuffer;
    }

    /**
     * Sends the message on the channel.
     */
    public void send(ByteBuffer message) throws IOException
    {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine("Received message with " + message.remaining() + " bytes");
        }
        myChannel.write(message);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Connection [myChannel=" + myChannel + "]";
    }
}
