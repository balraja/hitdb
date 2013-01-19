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

package org.hit.db.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hit.concurrent.LocklessSkipList;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.Schema;
import org.hit.db.transactions.Transactable;

/**
 * An implementation of a table wherein the keyspace of the table is
 * partitioned among multiple nodes.
 * 
 * @author Balraja Subbiah
 */
public class TransactablePartitionedTable<K extends Comparable<K>, P extends Persistable<K>>
    extends AbstractTransactableTable<K,P>
{
    private final LocklessSkipList<K, Transactable<K,P>> myIndex;
    
    /**
     * CTOR
     */
    public TransactablePartitionedTable(Schema schema)
    {
        super(schema);
        myIndex = new LocklessSkipList<K, Transactable<K,P>>(10);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addToTable(Transactable<K, P> transactable)
    {
        myIndex.add(transactable.getPersistable().primaryKey(),
                    transactable);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Transactable<K, P>>
        findMatching(Predicate<K, P> predicate,
                     K start,
                     K end,
                     long time,
                     long transactionID)
    {
        ArrayList<Transactable<K,P>> result = new ArrayList<>();
        LocklessSkipList<K, Transactable<K,P>>.SkipListIterator iterator =
            myIndex.lookupValues(start, end);
        
        while (iterator.hasNext()) {
            for (Transactable<K, P> transactable : iterator.next()) {
                if (transactable.isValid(time, transactionID)
                    && predicate.isInterested(transactable.getPersistable()))
                {
                    result.add(transactable);
                }
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Transactable<K, P>>
        findMatching(Predicate<K, P> predicate,
                     long time,
                     long transactionID)
    {
        ArrayList<Transactable<K,P>> result = new ArrayList<>();
        LocklessSkipList<K, Transactable<K,P>>.SkipListIterator iterator =
            myIndex.lookupAllValues();
        
        while (iterator.hasNext()) {
            for (Transactable<K, P> transactable : iterator.next()) {
                if (transactable.isValid(time, transactionID)
                    && predicate.isInterested(transactable.getPersistable()))
                {
                    result.add(transactable);
                }
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transactable<K, P> getRow(K key, long time, long transactionID)
    {
        List<Transactable<K, P>> result = myIndex.lookupValue(key);
        if (result != null) {
            for (Transactable<K, P> transactable : result) {
                if (transactable.isValid(time, transactionID)) {
                    return transactable;
                }
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(Transactable<K, P> transactable)
    {
        myIndex.remove(transactable.getPersistable().primaryKey(),
                       transactable);
    }
}
