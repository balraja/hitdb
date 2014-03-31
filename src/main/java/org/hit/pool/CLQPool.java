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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Defines the contract for pool of objects uses a {@link ConcurrentLinkedQueue}
 * for keeping track of pooled and allocated instances.
 * 
 * @author Balraja Subbiah
 */
public class CLQPool<T extends Poolable> extends AbstractPool<T>
{
    private final Set<T> myAllocatedInstances;
    
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
        myAllocatedInstances = 
            Collections.newSetFromMap(new IdentityHashMap<T,Boolean>(size));
        myFreeInstances      = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < initialSize; i++) {
            myFreeInstances.offer(newObject());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void surrender(T freedObject)
    {
        if (freedObject == null) {
            return;
        }
        if (myAllocatedInstances.remove(freedObject)) {
            myFreeInstances.offer(freedObject);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public T getObject()
    {
        T allocatedObject = myFreeInstances.poll();
        if (allocatedObject == null) {
            if (myAllocatedInstances.size() < getPoolSize()) {
                for (int i = 0; i < getInitialSize(); i++) {
                    myFreeInstances.offer(newObject());
                }
            }
        }
        allocatedObject = myFreeInstances.poll();
        if (allocatedObject != null) {
            myAllocatedInstances.add(allocatedObject);
        }
        else {
            allocatedObject = newObject();
        }
        return allocatedObject;
        
    }
}
