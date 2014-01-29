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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.buffer.BufferManager;
import org.hit.buffer.ManagedBuffer;
import org.hit.buffer.ManagedBufferInputStream;
import org.hit.buffer.ManagedBufferOutputStream;
import org.hit.util.LogFactory;

import com.google.common.primitives.Ints;

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
    public Collection<Message> parse(BinaryMessage binaryMessage)
    {
        try {
            ArrayList<Message> messages = new ArrayList<>();
            ManagedBufferInputStream min = 
                ManagedBufferInputStream.wrapSerializedData(
                    (ManagedBuffer) binaryMessage);
            
            byte[] sizeArray = new byte[4];
            min.read(sizeArray);
            int size = Ints.fromByteArray(sizeArray);
            
            while (size > 0) {
                min.setEOFMark(size);
                ObjectInputStream oStream = new ObjectInputStream(min);
                messages.add((Message) oStream.readObject());
                min.unsetEOFMark();
                
                int read = min.read(sizeArray);
                size = read == 4 ? Ints.fromByteArray(sizeArray) : -1;
            }
            min.close();
            return messages;
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BinaryMessage serialize(Message message)
    {
        try {
            ManagedBufferOutputStream mout = 
                new ManagedBufferOutputStream(myBufferManager, 4);
            ObjectOutputStream oStream = new ObjectOutputStream(mout);
            oStream.writeObject(message);
            oStream.close();
            
            mout.seekToFirst();
            mout.write(Ints.toByteArray(mout.getByteCount() - 4));
            return mout.getWrittenData();
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
    }
}
