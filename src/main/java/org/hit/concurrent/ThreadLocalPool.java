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

package org.hit.concurrent;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of a pool in which objects are cached in thread local
 * collection.
 * 
 * @author Balraja Subbiah
 */
public abstract class ThreadLocalPool<T extends Poolable>
extends AbstractPool<T>
{
    private final ThreadLocal<List<T>> myFreeObjects;

    private final ThreadLocal<List<T>> myUsedObjects;

    /**
     * CTOR
     */
    public ThreadLocalPool(final int size, final int initialSize)
    {
        super(size, initialSize);
        myFreeObjects = new ThreadLocal<List<T>>() {

            /**
             * {@inheritDoc}
             */
            @Override
            protected List<T> initialValue()
            {
                ArrayList<T> freeObjects = new ArrayList<>();
                for (int i = 0; i < initialSize; i++) {
                    freeObjects.add(newObject());
                }
                return freeObjects;
            }
        };

        myUsedObjects = new ThreadLocal<List<T>>() {

            /**
             * {@inheritDoc}
             */
            @Override
            protected List<T> initialValue()
            {
                return new ArrayList<>();
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getObject()
    {
        List<T> freeObjects = myFreeObjects.get();
        if (freeObjects.isEmpty()) {
            return null;
        }
        else {
            T object = freeObjects.remove(0);
            myUsedObjects.get().add(object);
            return object;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void surrender(T freedObject)
    {
        myUsedObjects.get().remove(freedObject);
        myFreeObjects.get().add(freedObject);
    }

}
