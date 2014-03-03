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

package org.hit.pool;

/**
 * Defines the contract for an abstract pool that can be used for caching
 * objects to avoid garbage collection.
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractPool<T extends Poolable> implements Pool<T>
{
    private final int myPoolSize;

    private final int myInitialSize;
    
    private final Class<T> myPoolableType;
    
    private final Factory myInstanceFactory;

    /**
     * CTOR
     */
    public AbstractPool(int        size, 
                        int        initialSize, 
                        Class<T>   poolableType,
                        Factory    factory)
    {
        myPoolSize        = size;
        myInitialSize     = initialSize;
        myPoolableType    = poolableType;
        myInstanceFactory = factory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free(T object)
    {
        object.free();
        surrender(object);
    }

    /**
     * Returns the value of initialSize
     */
    public int getInitialSize()
    {
        return myInitialSize;
    }

    /**
     * Returns the value of poolSize
     */
    public int getPoolSize()
    {
        return myPoolSize;
    }

    /** Creates a new instance of object of the specified type */
    protected T newObject()
    {
        T instance = myInstanceFactory.create(myPoolableType);
        instance.initialize();
        return instance;
    }

    /** Surrenders the object to the pool */
    protected abstract void surrender(T freedObject);
}
