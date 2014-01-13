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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.SocketChannel;
import java.util.List;

/**
 * @author Balraja Subbiah
 */
public class CompositeByteBuffer
{
    private List<ByteBuffer> myBinaryData;

    /**
     * CTOR
     */
    public CompositeByteBuffer(List<ByteBuffer> binaryData)
    {
        super();
        myBinaryData = binaryData;
    }
    
    /**
     * Writes the serialized data to the channels.
     */
    public void writeTo(SocketChannel outputChannel) throws IOException
    {
        for (ByteBuffer buffer : myBinaryData) {
            buffer.flip();
            outputChannel.write(buffer);
        }
    }
    
    /**
     * Writes the serialized data to the channels.
     */
    public void readFrom(SocketChannel outputChannel) throws IOException
    {
    }
}
