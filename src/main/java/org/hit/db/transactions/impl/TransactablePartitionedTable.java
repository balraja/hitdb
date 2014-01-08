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

package org.hit.db.transactions.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;

import org.hit.concurrent.LocklessSkipList;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.HitTableSchema;
import org.hit.db.transactions.Transactable;
import org.hit.db.transactions.ValidationResult;
import org.hit.util.Pair;

import com.google.common.collect.Collections2;

/**
 * An implementation of a table wherein the keyspace of the table is
 * partitioned linearly among multiple nodes.
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
    public TransactablePartitionedTable(HitTableSchema schema)
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
    
    private Collection<Transactable<K, P>> doFindMatching(
        Predicate        predicate,
        long             transactionID,
        long             time,
        LocklessSkipList<K, Transactable<K,P>>.SkipListIterator iterator)
    {
        TreeMap<K, Pair<ValidationResult, Transactable<K,P>>> result = 
                        new TreeMap<>();
        while (iterator.hasNext()) {
            for (Transactable<K,P> transactable : iterator.next()) {
                if (predicate.isInterested(transactable.getPersistable())) {
                    ValidationResult validationResult = 
                        transactable.validate(time, transactionID);
                    Pair<ValidationResult, Transactable<K,P>> sameKeyValue = 
                        result.get(transactable.getPersistable().primaryKey());
                    if (   sameKeyValue == null 
                        && validationResult.isValid()) 
                    {
                        result.put(transactable.getPersistable().primaryKey(),
                                   new Pair<>(validationResult, transactable));
                    }
                    else if (   validationResult.isSpeculativelyValid() 
                              && validationResult.getTransactionId() 
                                     != transactionID
                              && sameKeyValue.getFirst().getTransactionId()
                                     != transactionID
                              && sameKeyValue.getFirst().isSpeculativelyValid()
                              && validationResult.getTransactionId()
                                     > sameKeyValue.getFirst()
                                                   .getTransactionId())
                    {
                        result.put(transactable.getPersistable().primaryKey(),
                                   new Pair<>(validationResult, transactable));
                    }
                }
            }
        }
        
        return Collections2.transform(result.values(),
                                      new AddDependency(transactionID));
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Transactable<K, P>>
        findMatching(Predicate predicate,
                     long      time,
                     long      transactionID)
    {
        LocklessSkipList<K, Transactable<K,P>>.SkipListIterator iterator =
            myIndex.lookupAllValues();
        return doFindMatching(predicate, transactionID, time, iterator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Transactable<K, P>>
        findMatching(Predicate predicate,
                     K         start,
                     K         end,
                     long      time,
                     long      transactionID)
    {
        LocklessSkipList<K, Transactable<K,P>>.SkipListIterator iterator =
            myIndex.lookupValues(start, end);
        return doFindMatching(predicate, transactionID, time, iterator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transactable<K, P> getRow(K key, long time, long transactionID)
    {
        List<Transactable<K, P>> result = myIndex.lookupValue(key);
        return doGetRow(result, time, transactionID);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public long rowCount()
    {
        return myIndex.getCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transactable<K, P> deleteRow(K key, long time, long transactionID)
    {
        Transactable<K, P> row = getRow(key, time, transactionID);
        if (myIndex.remove(key, row)) {
            return row;
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Transactable<K, P>> deleteRange(
                                                      K start,
                                                      K end,
                                                      long time,
                                                      long transactionID)
    {
        List<Transactable<K,P>> result = new ArrayList<>();
        LocklessSkipList<K, Transactable<K,P>>.SkipListIterator iterator =
            myIndex.lookupValues(start, end);
        while (iterator.hasNext()) {
            List<Transactable<K,P>> rowVersions = iterator.next();
            Transactable<K,P> row = doGetRow(rowVersions, time, transactionID);
            if (myIndex.remove(row.getPersistable().primaryKey(), 
                               row))
            {
                result.add(row);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteVersion(Transactable<K, P> deletedVersion)
    {
        myIndex.remove(deletedVersion.getPersistable().primaryKey(), 
                       deletedVersion);
    }
}
