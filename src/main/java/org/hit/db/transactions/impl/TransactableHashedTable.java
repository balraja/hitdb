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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.hit.concurrent.HashTable;
import org.hit.concurrent.RefinableHashTable;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.Schema;
import org.hit.db.transactions.Transactable;

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
        
        private final long myTransactionID;
        
        private final long myTime;

        /**
         * CTOR
         */
        public BasicFilter(Predicate queryPredicate, 
                           long transactionID,
                           long time)
        {
            super();
            myQueryPredicate = queryPredicate;
            myTransactionID = transactionID;
            myTime = time;
        }
        
        public boolean apply(Transactable<K,P> transactable)
        {
            return transactable.isValid(myTime, myTransactionID)
                && myQueryPredicate.isInterested(transactable.getPersistable());
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
            long transactionID, 
            long time,
            K start,
            K end)
        {
            super(queryPredicate, transactionID, time);
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
                     long      time,
                     long      transactionID)
    {
        return doFindMatching(new RangeFilter(
            predicate,
            time,
            transactionID,
            start,
            end));
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Transactable<K, P>>
        findMatching(Predicate predicate, long time, long transactionID)
    {
        return doFindMatching(new BasicFilter(predicate, transactionID, time));
    }
    
    public Collection<Transactable<K, P>>
        doFindMatching(com.google.common.base.Predicate<Transactable<K,P>>
                           filteringPredicate)
    {
        List<Transactable<K,P>> result = new ArrayList<>();
        Iterator<Transactable<K,P>> itr = myIndex.getAllValues();
        while (itr.hasNext()) {
            Transactable<K,P> transactable = itr.next();
            if (filteringPredicate.apply(transactable))
            {
                result.add(transactable);
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
        List<Transactable<K, P>> result = myIndex.get(key);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public long rowCount()
    {
        return myIndex.count();
    }
}
