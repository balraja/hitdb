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

package org.hit.db.transactions.impl;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.hit.concurrent.HashTable;
import org.hit.concurrent.RefinableHashTable;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.Schema;
import org.hit.db.transactions.Transactable;
import org.hit.db.transactions.ValidationResult;
import org.hit.util.Pair;

import com.google.common.collect.Collections2;

/**
 * An implementation of a table wherein the keyspace is distributed on a
 * hash ring partitioned between the nodes.
 * 
 * @author Balraja Subbiah
 */
public class TransactableHashedTable <K extends Comparable<K>, P extends Persistable<K>>
    extends AbstractTransactableTable<K,P>
{
    private final HashTable<K, Transactable<K,P>> myIndex;
    
    private class BasicFilter implements 
        com.google.common.base.Predicate<Transactable<K,P>>
    {
        private final Predicate myQueryPredicate;

        /**
         * CTOR
         */
        public BasicFilter(Predicate queryPredicate)
        {
            super();
            myQueryPredicate = queryPredicate;
        }
        
        public boolean apply(Transactable<K,P> transactable)
        {
            return myQueryPredicate.isInterested(transactable.getPersistable());
        }
    }
    
    private class RangeFilter extends BasicFilter
    {
        private final K myStart;
        
        private final K myEnd;
        
        /**
         * CTOR
         */
        public RangeFilter(
            Predicate queryPredicate, 
            K start,
            K end)
        {
            super(queryPredicate);
            myStart = start;
            myEnd = end;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean apply(Transactable<K, P> transactable)
        {
            return super.apply(transactable)
                && (transactable.getPersistable()
                                .primaryKey()
                                .compareTo(myStart) >= 0)
                && (transactable.getPersistable()
                                .primaryKey()
                                .compareTo(myEnd) <= 0);
        }
    }

    /**
     * CTOR
     */
    public TransactableHashedTable(Schema schema)
    {
        super(schema);
        myIndex = new RefinableHashTable<>();
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
        findMatching(Predicate predicate,
                     K         start,
                     K         end,
                     long      transactionID,
                     long      time)
    {
        return doFindMatching(new RangeFilter(predicate, start, end),
                              time,
                              transactionID);
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Transactable<K, P>>
        findMatching(Predicate predicate, long time, long transactionID)
    {
        return doFindMatching(new BasicFilter(predicate), transactionID, time);
    }
    
    public Collection<Transactable<K, P>>
        doFindMatching(com.google.common.base.Predicate<Transactable<K,P>>
                           filteringPredicate,
                       long transactionID,
                       long time)
    {
        Iterator<Transactable<K,P>> itr = myIndex.getAllValues();
        TreeMap<K, Pair<ValidationResult, Transactable<K,P>>> result = 
            new TreeMap<>();
        while (itr.hasNext()) {
            Transactable<K,P> transactable = itr.next();
            if (filteringPredicate.apply(transactable)){
                
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
        
        return Collections2.transform(result.values(),
                                      new AddDependency(transactionID));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transactable<K, P> getRow(K key, long time, long transactionID)
    {
        List<Transactable<K, P>> result = myIndex.get(key);
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
        return myIndex.count();
    }
}
