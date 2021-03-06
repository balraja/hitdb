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

package org.hit.communicator;

import org.hit.io.ObjectIOFactory;
import org.hit.io.buffer.BufferManager;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * A simple factory for generating the <code>ObjectStreamSerializer</code>
 *
 * @author Balraja Subbiah
 */
public class ObjectStreamSerializerFactory implements SerializerFactory
{
    private final BufferManager myBufferManager;
    
    private final ObjectIOFactory myIOFactory;
    
    /**
     * CTOR
     */
    @Inject
    public ObjectStreamSerializerFactory(
        @Named("communicator") BufferManager bufferManager,
        ObjectIOFactory                      ioFactory)
    {
        myBufferManager = bufferManager;
        myIOFactory     = ioFactory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MessageSerializer makeSerializer()
    {
        return new ObjectStreamSerializer(myBufferManager, myIOFactory);
    }

}
