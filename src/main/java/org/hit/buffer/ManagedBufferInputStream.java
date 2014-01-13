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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.util.List;

/**
 * Extends {@link InputStream} to support reading from 
 * {@link SerializedData}.
 * 
 * @author Balraja Subbiah
 */
public class ManagedBufferInputStream extends InputStream
{
    private final BufferManager myBufferManager;
    
    private final List<ByteBuffer> myBinaryData;
    
    private int myBufferIndex;
    
    /**
     * CTOR
     */
    private ManagedBufferInputStream(BufferManager manager, 
                                     List<ByteBuffer> binaryData)
    {
        super();
        myBufferManager = manager;
        myBinaryData = binaryData;
        myBufferIndex = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException
    {
        if (!myBinaryData.get(myBufferIndex).hasRemaining()) {
            myBufferIndex++;
        }
        return myBinaryData.get(myBufferIndex).get();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        myBufferManager.free(myBinaryData);
    }
    
    /**
     * A factory method that reads data from {@link SocketChannel} and 
     * wraps it using {@link ManagedBufferInputStream} for further 
     * consumption.
     */
    public static ManagedBufferInputStream readAndWrapData(
        ReadableByteChannel inChannel, 
        int size, 
        BufferManager manager) throws IOException
    {
        List<ByteBuffer> toBeReadData = manager.getBuffer(size);
        for (ByteBuffer buffer : toBeReadData) {
            inChannel.read(buffer);
            buffer.flip();
        }
        return new ManagedBufferInputStream(manager, toBeReadData);
    }
}
