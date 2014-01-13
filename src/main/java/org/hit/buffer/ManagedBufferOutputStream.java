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
    
    /**
     * CTOR
     */
    public ManagedBufferOutputStream(BufferManager bufferManager)
    {
        super();
        myBufferManager = bufferManager;
        myWrittenBuffers = new ArrayList<>();
        myLastBuffer = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void write(int b) throws IOException
    {
        if (myLastBuffer == null || !myLastBuffer.hasRemaining()) {
            if (myLastBuffer != null) {
                // Flip the buffers so that they can be read easily.
                myLastBuffer.flip();
                myWrittenBuffers.add(myLastBuffer);
            }
            myLastBuffer = myBufferManager.getBuffer();
        }
        myLastBuffer.put((byte) b);
    }
    
    /** Returns data that has been written to this stream */
    public SerializedData getWrittenData()
    {
        if (myLastBuffer != null) {
            // Flip the buffers so that they can be read easily.
            myLastBuffer.flip();
            myWrittenBuffers.add(myLastBuffer);
        }
        return new SerializedData(myWrittenBuffers);
    }
}
