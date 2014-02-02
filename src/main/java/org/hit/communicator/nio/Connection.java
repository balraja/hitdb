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
import java.nio.channels.SocketChannel;

import org.hit.buffer.BufferManager;
import org.hit.buffer.ManagedBuffer;
import org.hit.communicator.BinaryMessage;

/**
 * An abstraction to capture the active connection between two machines.
 *
 * @author Balraja Subbiah
 */
public class Connection
{
    private final SocketChannel myChannel;
    
    private final BufferManager myBufferManager;
    
    private ManagedBuffer myReadBuffer;

    /**
     * CTOR
     */
    public Connection(SocketChannel channel, BufferManager bufferManager)
    {
        myChannel = channel;
        myBufferManager = bufferManager;
        myReadBuffer = null;
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
    public BinaryMessage read() throws IOException
    {
        if (myReadBuffer == null) {
            myReadBuffer = new ManagedBuffer(myBufferManager);
        }
        
        myReadBuffer.readFrom(myChannel);
        if (myReadBuffer.getBinaryData().isEmpty()) {
            return null;
        }
        else {
            BinaryMessage readMessage = myReadBuffer;
            myReadBuffer = null;
            return readMessage;
        }
    }


    /**
     * Sends the message on the channel.
     */
    public void send(BinaryMessage message) throws IOException
    {
        message.writeTo(myChannel);
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
