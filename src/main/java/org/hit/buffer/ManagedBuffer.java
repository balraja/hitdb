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
package org.hit.buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.communicator.BinaryMessage;
import org.hit.util.LogFactory;

/**
 * A collection of {@link ByteBuffer}s that represents some serialized data.
 * 
 * @author Balraja Subbiah
 */
public class ManagedBuffer implements BinaryMessage
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(ManagedBuffer.class);
                    
    private final BufferManager myBufferManager;
    
    private final List<ByteBuffer> myBinaryData;
    
    /**
     * CTOR
     */
    public ManagedBuffer(BufferManager manager)
    {
        super();
        myBufferManager = manager;
        myBinaryData = new ArrayList<>();
    }

    /**
     * CTOR
     */
    public ManagedBuffer(BufferManager manager, 
                         List<ByteBuffer> binaryData)
    {
        super();
        myBufferManager = manager;
        myBinaryData = binaryData;
    }

    /**
     * Returns the value of binaryData after clearing the existing list.
     */
    public List<ByteBuffer> getBinaryData()
    {
        return myBinaryData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeTo(WritableByteChannel channel) throws IOException
    {
        if (!myBinaryData.isEmpty()) {
            for (ByteBuffer buffer : myBinaryData) {
                channel.write(buffer);
            }
            myBufferManager.free(myBinaryData);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readFrom(ReadableByteChannel readableChannel) 
        throws IOException
    {
        ByteBuffer buffer = myBufferManager.getBuffer();
        int read = 0;
        while ((read = readableChannel.read(buffer)) > 0) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Read " + read + " bytes from the channel"
                         + " buffer " + buffer);
            }
            if (!buffer.hasRemaining()) {
                buffer.flip();
                myBinaryData.add(buffer);
                buffer = myBufferManager.getBuffer();
            }
        }
        
        if (read <= 0 && buffer.position() > 0) {
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Read " + read + " bytes from the channel"
                         + " buffer " + buffer);
            }
            buffer.flip();
            myBinaryData.add(buffer);
        }
        else {
            // This is an useless buffer. SO we are freeing it.
            myBufferManager.free(buffer);
        }
        
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("Read " + myBinaryData.size() + " buffers from the channel");
        }
    }
    
    /** Frees the buffers */
    public void free()
    {
        if (!myBinaryData.isEmpty()) {
            myBufferManager.free(myBinaryData);
        }
    }
}
