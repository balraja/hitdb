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

import java.util.ArrayList;
import java.util.List;

import org.hit.io.pool.PoolableRegistry;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * @author Balraja Subbiah
 */
public class SimplePoolableRegistry implements PoolableRegistry
{
    private static final List<Class<? extends Poolable>> OUR_LISTED_CLASSES =
        new ArrayList<>();
    
    private final BiMap<Class<?>, Integer> myTypeToIdentifierMap;
    
    /**
     * CTOR
     */
    public SimplePoolableRegistry()
    {
        myTypeToIdentifierMap = HashBiMap.<Class<?>, Integer>create();
        int identifier = 1;
        for (Class<?> poolableType : OUR_LISTED_CLASSES) {
            myTypeToIdentifierMap.put(poolableType, Integer.valueOf(identifier++));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUniqueIdentifier(Class<?> poolableType)
    {
        Integer integer = myTypeToIdentifierMap.get(poolableType);
        return integer!= null ? integer.intValue() : -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Class<?> getPoolableType(int identifier)
    {
        return myTypeToIdentifierMap.inverse().get(Integer.valueOf(identifier));
    }
}
