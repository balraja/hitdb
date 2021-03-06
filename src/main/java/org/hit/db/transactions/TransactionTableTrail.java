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

package org.hit.db.transactions;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hit.db.model.Persistable;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines the contract for a type that captures the various objects read/written
 * during a <code>Transaction</code> from a single table.
 * 
 * @author Balraja Subbiah
 */
public class TransactionTableTrail<K extends Comparable<K>,
                                   P extends Persistable<K>>
    implements Poolable
{
    private String                                    myTableName;
    
    private final Set<Transactable<K,P>>               myReadSet;

    private final Set<Transactable<K,P>>               myWriteSet;

    private final Set<Transactable<K,P>>               myNewWriteSet;
    
    private final Set<K>                               myDeleteSet;

    private final Map<PredicateWrapper<K>,
                      Collection<Transactable<K,P>>>   myPredicateToDataMap;
    
    /**
     * CTOR
     */
    public TransactionTableTrail()
    {
        myReadSet            = new HashSet<>();
        myWriteSet           = new HashSet<>();
        myNewWriteSet        = new HashSet<>();
        myPredicateToDataMap = new HashMap<>();
        myDeleteSet          = new HashSet<>();
    }
    
    /**
     * CTOR
     */
    public static <PK extends Comparable<PK>, T extends Persistable<PK>>
        TransactionTableTrail<PK,T> create(String tableName)
    {
        @SuppressWarnings("unchecked")
        TransactionTableTrail<PK, T> trail =
            PooledObjects.getInstance(TransactionTableTrail.class);
        trail.myTableName = tableName;
        return trail;
    }

    /**
     * Returns the value of newWriteSet
     */
    public Set<Transactable<K,P>> getNewWriteSet()
    {
        return myNewWriteSet;
    }

    /**
     * Returns the value of predicateToDataMap
     */
    public Map<PredicateWrapper<K>, Collection<Transactable<K, P>>>
        getPredicateToDataMap()
    {
        return myPredicateToDataMap;
    }

    /**
     * Returns the value of readSet
     */
    public Set<Transactable<K, P>> getReadSet()
    {
        return myReadSet;
    }

    /**
     * Returns the value of tableName
     */
    public String getTableName()
    {
        return myTableName;
    }
    
    /**
     * Setter for tableName
     */
    public void setTableName(String tableName)
    {
        myTableName = tableName;
    }

    /**
     * Returns the value of writeSet
     */
    public Set<Transactable<K,P>> getWriteSet()
    {
        return myWriteSet;
    }

    /**
     * Returns the value of deleteSet
     */
    public Set<K> getDeleteSet()
    {
        return myDeleteSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myTableName = null;
        myReadSet.clear();
        myWriteSet.clear();
        myNewWriteSet.clear();
        myDeleteSet.clear();
        myPredicateToDataMap.clear();
    }
}
