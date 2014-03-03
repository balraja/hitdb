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
package org.hit.pool;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.communicator.nio.NIOCommunicator;
import org.hit.util.LogFactory;

/**
 * Defines the contract for pool of objects uses a {@link ConcurrentLinkedQueue}
 * for keeping track of pooled and allocated instances.
 * 
 * @author Balraja Subbiah
 */
public class CLQPool<T extends Poolable> extends AbstractPool<T>
{
    private final List<T> myAllocatedInstances;
    
    private final ConcurrentLinkedQueue<T> myFreeInstances;
    
    /**
     * CTOR
     */
    public CLQPool(int      size, 
                   int      initialSize,
                   Class<T> instanceType,
                   Factory  factory)
    {
        super(size, initialSize, instanceType, factory);
        myAllocatedInstances = new CopyOnWriteArrayList<>();
        myFreeInstances      = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < initialSize; i++) {
            myFreeInstances.add(newObject());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void surrender(T freedObject)
    {
        myAllocatedInstances.remove(freedObject);
        myFreeInstances.add(freedObject);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public T getObject()
    {
        T allocatedObject = myFreeInstances.remove();
        myAllocatedInstances.add(allocatedObject);
        return allocatedObject;
    }
}
