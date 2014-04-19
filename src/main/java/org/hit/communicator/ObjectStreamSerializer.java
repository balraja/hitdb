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

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.io.ObjectIOFactory;
import org.hit.io.buffer.BufferManager;
import org.hit.io.buffer.ManagedBuffer;
import org.hit.io.buffer.ManagedBufferInputStream;
import org.hit.io.buffer.ManagedBufferOutputStream;
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
    
    private final BufferManager myBufferManager;
    
    private final ObjectIOFactory myIOFactory;
    
    /**
     * CTOR
     */
    public ObjectStreamSerializer(BufferManager manager, 
                                  ObjectIOFactory ioFactory)
    {
        myBufferManager = manager;
        myIOFactory     = ioFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Message> parse(BinaryMessage binaryMessage)
    {
        ArrayList<Message> messages = new ArrayList<>();
        try (
            ManagedBufferInputStream min = 
                ManagedBufferInputStream.wrapSerializedData(
                    (ManagedBuffer) binaryMessage))
        {
            
            byte[] sizeArray = new byte[4];
            min.read(sizeArray);
            int size = Ints.fromByteArray(sizeArray);
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("The message size is " + size);
            }
            while (size > 0) {
                min.setEOFMark(size);
                ObjectInput oInput = myIOFactory.getInput(min);
                messages.add((Message) oInput.readObject());
                min.unsetEOFMark();
                
                int read = min.read(sizeArray);
                size = read == 4 ? Ints.fromByteArray(sizeArray) : -1;
            }
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return messages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BinaryMessage serialize(Message message)
    {
        BinaryMessage result = null;
        try (
            ManagedBufferOutputStream mout = 
                new ManagedBufferOutputStream(myBufferManager, 4))
        {
            ObjectOutput oOutput = myIOFactory.getOutput(mout);
            oOutput.writeObject(message);
            oOutput.close();
            mout.seekToFirst();
            mout.write(Ints.toByteArray(mout.getByteCount() - 4));
            result = mout.getWrittenData();
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return result;
    }
}
