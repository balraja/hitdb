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

import java.util.Collection;

/**
 * An util class to help releasing freed instances.
 * 
 * @author Balraja Subbiah
 */
public final class PoolUtils
{
    /** A helper method to free instances of objects in a collection */
    public static <T> void free(Collection<T> freedInstances)
    {
        T instance = freedInstances.iterator().next();
        boolean isPoolable = instance.getClass().isAssignableFrom(Poolable.class);
        if (isPoolable) {
            for (T freedInstance : freedInstances) {
                PooledObjects.freeInstance((Poolable) freedInstance);
            }
        }
    }
    
    /** A helper method to free an instance if it's poolable */
    public static<T> void free(T instance)
    {
        if (instance.getClass().isAssignableFrom(Poolable.class)) {
            PooledObjects.freeInstance((Poolable) instance);
        }
    }
    
    /**
     * Pvt CTOR to avoid initialization.
     */
    private PoolUtils()
    {
    }
}
