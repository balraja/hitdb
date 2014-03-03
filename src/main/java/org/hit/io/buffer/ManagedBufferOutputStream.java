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
package org.hit.io.buffer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Extends {@link OutputStream} wherein data is written to managed 
 * {@link ByteBuffer}.
 * 
 * @author Balraja Subbiah
 */
public class ManagedBufferOutputStream extends OutputStream
{
    private final BufferManager myBufferManager;
    
    private final List<ByteBuffer> myWrittenBuffers;
    
    private ByteBuffer myLastBuffer;
    
    private int myByteCount;
    
    private int myInitialOffset;
    
    private int myRewriteBufferIndex;
    
    /**
     * CTOR
     */
    public ManagedBufferOutputStream(BufferManager bufferManager)
    {
        this(bufferManager, -1);
    }
    
    /**
     * CTOR
     */
    public ManagedBufferOutputStream(BufferManager bufferManager,
                                     int           initialOffset)
    {
        super();
        myBufferManager = bufferManager;
        myWrittenBuffers = new ArrayList<>();
        myLastBuffer = null;
        myByteCount = 0;
        myInitialOffset = initialOffset;
        myRewriteBufferIndex = -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(int b) throws IOException
    {
        if (myInitialOffset > 0) {
            
            myWrittenBuffers.addAll(myBufferManager.getBuffer(myInitialOffset));
            
            int bufferIndex = myInitialOffset / BufferManager.BUFFER_SIZE;
            if (myInitialOffset % BufferManager.BUFFER_SIZE > 0) {
                myLastBuffer = myWrittenBuffers.remove(bufferIndex);
                myLastBuffer.position(
                    myInitialOffset % BufferManager.BUFFER_SIZE);
            }
            else {
                myLastBuffer = myWrittenBuffers.get(bufferIndex - 1);
            }
            
            myInitialOffset = -1;
        }
        
        if (myLastBuffer == null || !myLastBuffer.hasRemaining()) {
            if (myLastBuffer != null && myRewriteBufferIndex < 0) {
               cacheCurrentBuffer();
            }
            
            if (myRewriteBufferIndex < 0) {
                myLastBuffer = myBufferManager.getBuffer();
            }
            else {
                if (myRewriteBufferIndex >= myWrittenBuffers.size()) {
                    throw new IOException(
                        "Overwriting data more than the current size");
                }
                
                myLastBuffer = myWrittenBuffers.get(myRewriteBufferIndex);
                myRewriteBufferIndex++;
            }
        }
        myLastBuffer.put((byte) b);
    }
    
    private void cacheCurrentBuffer()
    {
        // Flip the buffers so that they can be read easily.
        myLastBuffer.flip();
        myByteCount += (myLastBuffer.limit() + 1);
        myWrittenBuffers.add(myLastBuffer);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        if (myLastBuffer != null) {
            // Flip the buffers so that they can be read easily.
            if (myRewriteBufferIndex < 0) {
                cacheCurrentBuffer();
            }
            else {
                myLastBuffer.position(0);
            }
            myLastBuffer = null;
        }
    }
    
    /**
     * A helper method to reset the 
     */
    public void seekToFirst() throws IOException
    {
        if (myWrittenBuffers.isEmpty()) {
            throw new IOException("Buffers are empty.");
        }
        
        myRewriteBufferIndex = 0;
    }
    
    /**
     * Returns the number of bytes that has been written to 
     */
    public int getByteCount()
    {
        return myByteCount;
    }
    
    /** Returns data that has been written to this stream */
    public ManagedBuffer getWrittenData()
    {
        close();
        return new ManagedBuffer(myBufferManager, myWrittenBuffers);
    }
}
