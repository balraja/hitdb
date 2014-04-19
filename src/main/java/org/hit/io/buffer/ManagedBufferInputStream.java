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
import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.util.LogFactory;

/**
 * Extends {@link InputStream} to support reading from 
 * {@link ManagedBuffer}.
 * 
 * @author Balraja Subbiah
 */
public class ManagedBufferInputStream extends InputStream
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(ManagedBufferInputStream.class);
            
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
        if (myMark > EOF) {
            myReadBytes++;
            if (myReadBytes > myMark) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("We have read upto the mark");
                }
                return EOF;
            }
        }
        
        if (myBufferIndex < myBuffer.getBinaryData().size()) {
            byte val = myBuffer.getBinaryData().get(myBufferIndex).get();
            if (!myBuffer.getBinaryData().get(myBufferIndex).hasRemaining()) {
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("The current buffer has " 
                             + myBuffer.getBinaryData().get(myBufferIndex));
                    LOG.finest("Increasing the buffer index " + myBufferIndex + 1);
                }
                myBufferIndex += 1;
            }
            return (val & 0xff);
        }
        else {
            return EOF;
        }
    }
    
    /** Sets the end of this stream after n bytes */
    public void setEOFMark(int nBytes)
    {
        myMark = nBytes;
        myReadBytes = 0;
        if (LOG.isLoggable(Level.FINE)) {
            LOG.finest("Setting mark to " + myMark);
        }
    }
    
    /** Removes mark from the input stream */
    public void unsetEOFMark()
    {
        if (LOG.isLoggable(Level.FINEST)) {
            LOG.finest("UnSetting mark @ " + myMark + " read bytes " + myReadBytes);
        }
        myMark = EOF;
        myReadBytes = 0;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws IOException
    {
        myBuffer.free();
        super.close();
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
