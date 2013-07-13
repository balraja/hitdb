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

import org.hit.communicator.NodeID;
import org.hit.util.LogFactory;

/**
 * An abstraction to capture the active connection between two machines.
 * 
 * @author Balraja Subbiah
 */
public class Connection
{
    private static final int BUFFER_SIZE = 1024 * 1024;
    
    private static final String CONNECTION = "Connection_";
    
    private final SocketChannel myChannel;
    
    private final Logger myLogger;
    
    /**
     * CTOR
     */
    public Connection(SocketChannel channel, NodeID nodeID)
    {
        myChannel = channel;
        myLogger = LogFactory.getInstance().getLogger(CONNECTION + nodeID);
    }
    
    /**
     * Sends the message on the channel.
     */
    public void send(ByteBuffer message) throws IOException
    {
        myChannel.write(message);
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
        
        while ((readBytes = myChannel.read(buffer)) != -1) {
            buffer.flip();
            if (myLogger.isLoggable(Level.FINE)) {
                myLogger.fine("Message Buffer " + messageBuffer.position()
                         + " cap " + messageBuffer.capacity()
                         + " limit " + messageBuffer.limit()
                         + " rem " + messageBuffer.remaining());

                myLogger.fine("Data buffer " + buffer.remaining());
            }
            
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
        if (myLogger.isLoggable(Level.FINE)) {
            myLogger.fine("Resizing message buffer");
        }
        buffer.flip();
        ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2);
        newBuffer.put(buffer);
        return newBuffer;
    }
    
    /**
     * Closes the connection.
     */
    public void close() throws IOException
    {
        myChannel.close();
    }
}
