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
import java.util.List;

/**
 * A collection of {@link ByteBuffer}s that represents some serialized data.
 * 
 * @author Balraja Subbiah
 */
public class SerializedData
{
    private final BufferManager myManager;
    
    private final List<ByteBuffer> myBinaryData;

    /**
     * CTOR
     */
    public SerializedData(BufferManager manager, List<ByteBuffer> binaryData)
    {
        super();
        myManager = manager;
        myBinaryData = binaryData;
    }
    
    /**
     * CTOR
     * @throws IOException 
     */
    public SerializedData() throws IOException
    {
        super();
        myManager = null;
        myBinaryData = manager.getBuffer(size);
        for (ByteBuffer buffer : myBinaryData) {
            inChannel.read(buffer);
        }
    }
    
    /**
     * Writes the serialized data to the channels.
     */
    public void writeTo(WritableByteChannel outputChannel) throws IOException
    {
        for (ByteBuffer buffer : myBinaryData) {
            outputChannel.write(buffer);
        }
        if (myManager != null) {
            myManager.free(myBinaryData);
        }
    }
}
