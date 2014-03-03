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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.util.LogFactory;

/**
 * Defines the contract for a type that's responsible managing the 
 * object pools.
 * 
 * @author Balraja Subbiah
 */
public final class PooledObjects
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(PooledObjects.class);

    private static final int DEFAULT_INITIAL_SIZE = 10;
    
    private static final int DEFAULT_SIZE = 20;
    
    private static final Map<Class<?>, Pool<?>> ourTypeToPoolMap = 
        new ConcurrentHashMap<>();
    
    /**
     * CTOR
     */
    private PooledObjects()
    {
    }
    
    /**
     * Returns an instance of the given type from the pool.
     */
    @SuppressWarnings("unchecked")
    public static Poolable getUnboundedInstance(Class<?> instanceType)
    {
        return (Poolable) getInstance((Class<? extends Poolable>) instanceType);
    }
    
    /**
     * Returns an instance of the given type from the pool.
     */
    @SuppressWarnings("unchecked")
    public static <T extends Poolable> T getInstance(Class<T> instanceType)
    {
        Pool<T> pool = (Pool<T>) ourTypeToPoolMap.get(instanceType);
        if (pool == null) {
            PoolConfiguration configuration = 
                instanceType.getAnnotation(PoolConfiguration.class);
            
            if (configuration == null) {
                pool = new CLQPool<T>(DEFAULT_INITIAL_SIZE,
                                      DEFAULT_SIZE,
                                      instanceType,
                                      new ReflectiveFactory());
            }
            else {
                try {
                    pool = new CLQPool<T>(configuration.initialSize(),
                                          configuration.size(),
                                          instanceType,
                                          configuration.factoryClass()
                                                       .newInstance());
                }
                catch (InstantiationException | IllegalAccessException e) {
                    LOG.log(Level.SEVERE, e.getMessage(), e);
                }
            }
            ourTypeToPoolMap.put(instanceType, pool);
        }
        return pool.getObject();
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends Poolable> void freeInstance(T freedInstance)
    {
        Pool<T> pool = (Pool<T>) ourTypeToPoolMap.get(freedInstance.getClass());
        if (pool == null) {
            return;
        }
        else {
            pool.free(freedInstance);
        }
    }
}
