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
package org.hit.concurrent.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Defines the contract for a type that's responsible managing the 
 * object pools.
 * 
 * @author Balraja Subbiah
 */
public abstract class PooledObjects
{
    private static final int DEFAULT_INITIAL_SIZE = 10;
    
    private static final int DEFAULT_SIZE = 20;
    
    private final Map<Class<?>, Pool<?>> myTypeToPoolMap;
    
    /**
     * CTOR
     */
    public PooledObjects()
    {
        myTypeToPoolMap = new ConcurrentHashMap<>();
    }
    
    /**
     * Returns an instance of the given type from the pool.
     */
    @SuppressWarnings("unchecked")
    public <T extends Poolable> T getInstance(Class<T> instanceType)
    {
        Pool<T> pool = (Pool<T>) myTypeToPoolMap.get(instanceType);
        if (pool == null) {
            PoolConfiguration configuration = 
                instanceType.getAnnotation(PoolConfiguration.class);
            
            if (configuration == null) {
                pool = new CLQPool<T>(DEFAULT_INITIAL_SIZE,
                                      DEFAULT_SIZE,
                                      instanceType);
            }
            else {
                pool = new CLQPool<T>(configuration.initialSize(),
                                      configuration.size(),
                                      instanceType);
            }
            myTypeToPoolMap.put(instanceType, pool);
        }
        return pool.getObject();
    }
    
    @SuppressWarnings("unchecked")
    public <T extends Poolable> void freeInstance(T freedInstance)
    {
        Pool<T> pool = (Pool<T>) myTypeToPoolMap.get(freedInstance.getClass());
        if (pool == null) {
            return;
        }
        else {
            pool.free(freedInstance);
        }
    }
}
