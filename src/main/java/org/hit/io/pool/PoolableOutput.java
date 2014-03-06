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

import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.hit.pool.Internable;
import org.hit.pool.Interner;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines the contract for a OutputStream that's responsible for 
 * releasing poolable Objects after they were serialized.
 * 
 * 
 * @author Balraja Subbiah
 */
public class PoolableOutput extends DataOutputStream
    implements ObjectOutput
{
    private final PoolableRegistry myRegistry;

    /**
     * CTOR
     */
    public PoolableOutput(OutputStream out, PoolableRegistry registry)
    {
        super(out);
        myRegistry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeObject(Object obj) throws IOException
    {
        if (obj instanceof Externalizable && obj instanceof Poolable) {
            int id = myRegistry.getUniqueIdentifier(obj.getClass());
            if (id >= 0) {
                writeInt(id);
                Externalizable externalizable = (Externalizable) obj;
                externalizable.writeExternal(this);
                PooledObjects.freeInstance((Poolable) obj); 
            }
            else {
                throw new IOException(obj.getClass().getName() 
                    + " hasn't been registered with the object registry");
            }
        }
        else if (obj instanceof Internable) {
            int id = myRegistry.getUniqueIdentifier(obj.getClass());
            if (id >= 0) {
                writeInt(id);
                Interner<?> interner = Interner.getInterner(obj.getClass());
                interner.writeToOutput(this, obj);
            }
        }
        else {
            throw new IOException(
                    obj.getClass().getSimpleName() +  " isn't Poolable & "
                    + "Externalizable. Only Poolable & Externalizable objects "
                    + "are allowed");

        }
    }
}
