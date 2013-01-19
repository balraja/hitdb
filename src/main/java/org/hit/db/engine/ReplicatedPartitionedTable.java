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

package org.hit.db.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hit.concurrent.LocklessSkipList;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.Schema;

/**
 * Defines the contract for the partitoned table that's replicated across
 * a different node.
 * 
 * @author Balraja Subbiah
 */
public class ReplicatedPartitionedTable <K extends Comparable<K>,
                                         P extends Persistable<K>>

    extends AbstractReplicatedTable<K,P>
{
    private final LocklessSkipList<K, P> myIndex;

    /**
     * CTOR
     */
    public ReplicatedPartitionedTable(Schema schema)
    {
        super(schema);
        myIndex = new LocklessSkipList<K,P>(10);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> findMatching(Predicate<K, P> predicate)
    {
        List<P> result = new ArrayList<>();
        LocklessSkipList<K,P>.SkipListIterator iterator =
            myIndex.lookupAllValues();
        while(iterator.hasNext()) {
            for (P p : iterator.next()) {
                if (predicate.isInterested(p)) {
                    result.add(p);
                }
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P>
        findMatching(Predicate<K, P> predicate, K start, K end)
    {
        List<P> result = new ArrayList<>();
        LocklessSkipList<K,P>.SkipListIterator iterator =
            myIndex.lookupValues(start, end);
        while(iterator.hasNext()) {
            for (P p : iterator.next()) {
                if (predicate.isInterested(p)) {
                    result.add(p);
                }
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P getRow(K primarykey)
    {
        List<P> values =  myIndex.lookupValue(primarykey);
        return (values != null && !values.isEmpty()) ? values.get(0)
                                                     : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean update(P old, P updated)
    {
        boolean removed = myIndex.remove(old.primaryKey(), old);
        boolean added  = myIndex.add(updated.primaryKey(), updated);
        return removed && added;
    }
}
