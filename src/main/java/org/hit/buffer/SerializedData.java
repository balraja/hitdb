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
import java.nio.channels.WritableByteChannel;
import java.util.ArrayList;
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
     * Writes the serialized data to the channels and fress the 
     * {@link ByteBuffer}s back to the pool.
     */
    public void drainTo(WritableByteChannel outputChannel) throws IOException
    {
        for (ByteBuffer buffer : myBinaryData) {
            outputChannel.write(buffer);
        }
        if (myManager != null) {
            myManager.free(myBinaryData);
        }
    }

    /**
     * Returns the value of binaryData after clearing the existing list.
     */
    public List<ByteBuffer> getAndClearBinaryData()
    {
        List<ByteBuffer> result = new ArrayList<>(myBinaryData);
        myBinaryData.clear();
        return result;
    }

    /**
     * Returns the value of manager
     */
    public BufferManager getManager()
    {
        return myManager;
    }
}
