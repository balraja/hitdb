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
import java.util.Iterator;
import java.util.List;

import org.hit.concurrent.HashTable;
import org.hit.concurrent.RefinableHashTable;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.Schema;

/**
 * Defines the contract for the hashed table that's replicated across a
 * dfifferent node.
 *
 * @author Balraja Subbiah
 */
public class ReplicatedHashedTable <K extends Comparable<K>,
                                    P extends Persistable<K>>
    extends AbstractReplicatedTable<K,P>
{
    private final HashTable<K, P> myIndex;

    /**
     * CTOR
     */
    public ReplicatedHashedTable(Schema schema)
    {
        super(schema);
        myIndex = new RefinableHashTable<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> findMatching(Predicate<K, P> predicate)
    {
        List<P> result = new ArrayList<>();
        Iterator<P> iterator = myIndex.getAllValues();
        while(iterator.hasNext()) {
            P value = iterator.next();
            if (predicate.isInterested(value)) {
                result.add(value);
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
        throw new UnsupportedOperationException(
            "On a hashed table we cann't iterate between range of keys");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public P getRow(K primarykey)
    {
        List<P> result = myIndex.get(primarykey);
        return (result != null && !result.isEmpty()) ? result.get(0)
                                                     : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean update(P old, P updated)
    {
        boolean removed  =
            old != null ? myIndex.remove(old.primaryKey(), old) : true;
        boolean added = myIndex.add(updated.primaryKey(), updated);
        return removed && added;
    }

}
