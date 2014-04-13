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

import org.hit.db.model.Predicate;
import org.hit.pool.PoolUtils;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * A type for capturing the result of query so that it will help us to 
 * compare the results.
 * 
 * @author Balraja Subbiah
 */
public class PredicateWrapper<K extends Comparable<K>> implements Poolable
{
    private Predicate myPredicate;

    private K         myStart;

    private K         myEnd;

    /**
     * Method for initializing the predicate wrappers.
     */
    public static <T extends Comparable<T>> PredicateWrapper<T> create(
        Predicate predicate)
    {
        return create(predicate, null, null);
    }

    /**
     * Factory method for creating an instance of <code>PredicateWrapper</code> 
     * and populating with various parameters.
     */
    public static <T extends Comparable<T>> PredicateWrapper<T> create(
        Predicate predicate, 
        T         start,
        T         end)
    {
        @SuppressWarnings("unchecked")
        PredicateWrapper<T> wrapper = 
            PooledObjects.getInstance(PredicateWrapper.class);
        wrapper.myPredicate = predicate;
        wrapper.myStart = start;
        wrapper.myEnd = end;
        return wrapper;
    }
    
    /**
     * Returns the value of predicate
     */
    public Predicate getPredicate()
    {
        return myPredicate;
    }

    /**
     * Returns the value of start
     */
    public K getStart()
    {
        return myStart;
    }

    /**
     * Returns the value of end
     */
    public K getEnd()
    {
        return myEnd;
    }

    public boolean isRangeQuery()
    {
        return myStart != null && myEnd != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        PoolUtils.free(myPredicate);
        PoolUtils.free(myStart);
        PoolUtils.free(myEnd);
        
        myPredicate = null;
        myStart     = null;
        myEnd       = null;
    }
}
