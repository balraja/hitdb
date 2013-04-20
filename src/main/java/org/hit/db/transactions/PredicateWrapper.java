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

import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;

/**
 * @author Balraja Subbiah
 */
public class PredicateWrapper<K extends Comparable<K>, P extends Persistable<K>>
{
    private final Predicate myPredicate;

    private final K         myStart;

    private final K         myEnd;

    /**
     * CTOR
     */
    public PredicateWrapper(Predicate predicate)
    {
        this(predicate, null, null);
    }

    /**
     * CTOR
     */
    public PredicateWrapper(Predicate predicate, 
                            K         start,
                            K         end)
    {
        super();
        myPredicate = predicate;
        myStart = start;
        myEnd = end;
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
}
