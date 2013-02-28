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
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

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

    /**
     * {@inheritDoc}
     */
    @Override
    public Message parse(ByteBuffer binaryMessage)
    {
        try {
            ByteArrayInputStream byteStream =
                            new ByteArrayInputStream(binaryMessage.array());
            ObjectInputStream oStream = new ObjectInputStream(byteStream);
            return (Message) oStream.readObject();
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
    public ByteBuffer serialize(Message message)
    {
        try {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            ObjectOutputStream oStream = new ObjectOutputStream(byteStream);
            oStream.writeObject(message);
            return ByteBuffer.wrap(byteStream.toByteArray());
        }
        catch (Exception e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            return null;
        }
    }
}
