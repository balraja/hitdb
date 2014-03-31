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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.pool.Internable;
import org.hit.pool.Interner;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;
import org.hit.util.LogFactory;

/**
 * Defines an input stream that initializes the object from pool.
 * 
 * @author Balraja Subbiah
 */
public class PoolableInput extends DataInputStream
    implements ObjectInput
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(PoolableInput.class);
                    
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
        boolean hasIdentifier = readBoolean();
        Class<?> type = 
            hasIdentifier ? myRegistry.getPoolableType(readInt())
                          : Class.forName(readUTF());
            
        if (Internable.class.isAssignableFrom(type)) {
           Interner<?> interner = Interner.getInterner(type);
           return interner.readFromInput(this);
        }
        else if (Poolable.class.isAssignableFrom(type)){
            Externalizable externalizable = 
                (Externalizable) 
                    PooledObjects.getUnboundedInstance(type);
            externalizable.readExternal(this);
            return externalizable;
        }
        else {
            try {
                Externalizable externalizable = 
                    (Externalizable) type.newInstance();
                externalizable.readExternal(this);
                return externalizable;
            }
            catch (InstantiationException | IllegalAccessException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
            return null;
        }
    }
}
