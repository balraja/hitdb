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
package org.hit.io.pool;

import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;

import org.hit.pool.Internable;
import org.hit.pool.Interner;
import org.hit.pool.PooledObjects;

/**
 * Defines an input stream that initializes the object from pool.
 * 
 * @author Balraja Subbiah
 */
public class PoolableInput extends DataInputStream
    implements ObjectInput
{
    private final PoolableRegistry myRegistry;

    /**
     * CTOR
     */
    public PoolableInput(InputStream in, PoolableRegistry registry)
    {
        super(in);
        myRegistry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object readObject() throws ClassNotFoundException, IOException
    {
        int identifier = readInt();
        Class<?> type = myRegistry.getPoolableType(identifier);
        if (Internable.class.isAssignableFrom(type)) {
           Interner<?> interner = Interner.getInterner(type);
           return interner.readFromInput(this);
        }
        else {
            Externalizable externalizable = 
                (Externalizable) 
                    PooledObjects.getUnboundedInstance(type);
            externalizable.readExternal(this);
            return externalizable;
        }
    }
}
