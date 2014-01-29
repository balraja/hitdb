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

/**
 * Extends {@link InputStream} to support reading from 
 * {@link ManagedBuffer}.
 * 
 * @author Balraja Subbiah
 */
public class ManagedBufferInputStream extends InputStream
{
    private final int EOF = -1;
    
    private final ManagedBuffer myBuffer;
     
    private int myBufferIndex;
    
    private int myMark;
    
    private int myReadBytes;
    
    /**
     * CTOR
     */
    private ManagedBufferInputStream(ManagedBuffer buffer)
    {
        myBuffer = buffer;
        myBufferIndex = 0;
        myMark = -1;
        myReadBytes = 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int read() throws IOException
    {
        if (myMark > -1) {
            myReadBytes++;
            if (myReadBytes >= myMark) {
                return EOF;
            }
        }
        
        if (!myBuffer.getBinaryData().get(myBufferIndex).hasRemaining()) {
            myBufferIndex += 1;
        }
        
        if (myBufferIndex >= myBuffer.getBinaryData().size()) {
            return EOF;
        }
        
        byte val = myBuffer.getBinaryData().get(myBufferIndex).get();
        return (val & 0xff);
    }
    
    /** Sets the end of this stream after n bytes */
    public void setEOFMark(int nBytes)
    {
        myMark = nBytes;
        myReadBytes = 0;
    }
    
    /** Removes mark from the input stream */
    public void unsetEOFMark()
    {
        myMark = EOF;
        myReadBytes = 0;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        myBuffer.free();
    }

    /**
     * A factory method to wrap the binary data present in the 
     * {@link ManagedBuffer} with {@link ManagedBufferInputStream}.
     */
    public static ManagedBufferInputStream wrapSerializedData(
        ManagedBuffer data)
    {
        return new ManagedBufferInputStream(data);
    }
}
