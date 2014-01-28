/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

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

package org.hit.communicator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.buffer.BufferManager;
import org.hit.buffer.ManagedBuffer;
import org.hit.buffer.ManagedBufferInputStream;
import org.hit.buffer.ManagedBufferOutputStream;
import org.hit.channel.ChannelInterface;
import org.hit.util.LogFactory;

/**
 * An implementation of <code>MessageSerializer</code> that uses the
 * Serializable/Externalizable interface for serializing the objects.
 *
 * @author Balraja Subbiah
 */
public class ObjectStreamSerializer implements MessageSerializer
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(ObjectStreamSerializer.class);
    
    private static final int LENGTH_FIELD_OFFSET = 4;
    
    private final BufferManager  myBufferManager;
    
    /**
     * CTOR
     */
    public ObjectStreamSerializer(BufferManager manager)
    {
        myBufferManager = manager;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Message> parse(ManagedBuffer binaryMessage)
    {
        try {
            ArrayList<Message> messages = new ArrayList<>();
            ManagedBufferInputStream min = 
                ManagedBufferInputStream.wrapSerializedData(binaryMessage);
            DataInputStream din = new DataInputStream(min);
            int size = -1;
            while ((size = din.readInt()) > 0) {
                min.setEOFMark(size);
                ObjectInputStream oStream = new ObjectInputStream(min);
                messages.add((Message) oStream.readObject());
                min.unsetEOFMark();
            }
            min.close();
            return messages;
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ManagedBuffer serialize(Message message)
    {
        try {
            ManagedBufferOutputStream mout = 
                new ManagedBufferOutputStream(myBufferManager);
            ObjectOutputStream oStream = new ObjectOutputStream(mout);
            oStream.writeObject(message);
            oStream.close();
            return mout.getWrittenData();
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
    }
}
