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

package org.hit.concurrent;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicMarkableReference;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * An implementation of linked hash table which minimizes the locking overhead
 * when accessing the data in the hash table.
 *
 * @author Balraja Subbiah
 */
public class RefinableHashTable<K, V> implements HashTable<K, V>
{
    private static final int INIT_LOCK_SIZE = 10;

    private static final int INIT_TABLE_SIZE = 100;

    private static final int NO_POSITION = -1;

    /**
     * The data stored in the hash map. We allow multiple keys with the same
     * remainder when modulo by table size. But at a given remainder each key
     * should be unique.
     */
    private List<ListMultimap<K, V>> myData;

    /** The list of locks that gaurds various portion of the tables */
    private List<ReentrantLock> myLocks;

    /** The thread that has acquired permission for resizing the table */
    private final AtomicMarkableReference<Thread> myOwner;

    /** The size of the set */
    private final AtomicInteger mySize;
    
    /** The number of values stored in this hash table */
    private final AtomicLong myCount;

    /**
     * CTOR
     */
    public RefinableHashTable()
    {
        myLocks = new ArrayList<>(INIT_LOCK_SIZE);
        myData = new ArrayList<>(INIT_TABLE_SIZE);
        myOwner = new AtomicMarkableReference<Thread>(null, false);
        mySize = new AtomicInteger(0);

        for (int i = 0; i < INIT_LOCK_SIZE; i++) {
            myLocks.add(new ReentrantLock());
        }

        for (int i = 0; i < INIT_TABLE_SIZE; i++) {
            myData.add(LinkedListMultimap.<K,V>create());
        }
        
        myCount = new AtomicLong(0L);
    }

    private int acquireLock(K key)
    {
        boolean[] mark = new boolean[] { false };
        Thread currentThread = Thread.currentThread();
        Thread owner = myOwner.get(mark);
        while (owner != null && owner != currentThread && mark[0]) {
            // Some other thread is resizing. Spin till resizing is over.
            owner = myOwner.get(mark);
        }
        int tablePos = key.hashCode() % myData.size();
        List<ReentrantLock> oldLocks = myLocks;
        ReentrantLock oldLock = oldLocks.get(tablePos % oldLocks.size());
        oldLock.lock();
        mark = new boolean[] { false };
        owner = myOwner.get(mark);

        if (mark[0] || myLocks != oldLocks) {
            oldLock.unlock();
            return NO_POSITION;
        }
        else {
            return tablePos;
        }
    }

    /** Adds an element corresponding to a given key to the map */
    @Override
    public boolean add(K key, V value)
    {
        int tablePos = acquireLock(key);
        if (tablePos == NO_POSITION) {
            return false;
        }
        ListMultimap<K, V> posData = myData.get(tablePos);
        posData.put(key, value);
        int size = mySize.getAndIncrement();
        if (size / myData.size() > THRESHOLD) {
            resize();
        }
        posData.put(key, value);
        myCount.incrementAndGet();
        releaseLock(tablePos);
        return true;
    }

    /** Returns an element corresponding to a given key to the map */
    @Override
    public List<V> get(K key)
    {
        int tablePos = acquireLock(key);
        if (tablePos == NO_POSITION) {
            return null;
        }
        ListMultimap<K, V> posData = myData.get(tablePos);
        return posData.get(key);
    }

    /**
     * {@inheritDoc}
     *
     * Please note that this is a very costly operation to be performed.
     */
    @Override
    public Iterator<V> getAllValues()
    {
        ArrayList<V> values = new ArrayList<>();
        Thread currentThread = Thread.currentThread();
        if (myOwner.compareAndSet(null, currentThread, false, true)) {
            // Spin till we acquire all the locks.
            for (ReentrantLock lock : myLocks) {
                while (lock.isLocked()) {
                }
            }

            for (ListMultimap<K, V> multimap : myData) {
                values.addAll(multimap.values());
            }
        }
        return values.iterator();
    }

    private void releaseLock(int tablePos)
    {
        ReentrantLock oldLock = myLocks.get(tablePos % myLocks.size());
        oldLock.unlock();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(K key, V value)
    {
        int tablePos = acquireLock(key);
        if (tablePos == NO_POSITION) {
            return false;
        }
        ListMultimap<K, V> posData = myData.get(tablePos);
        boolean isRemoved = posData.remove(key, value);
        if (isRemoved) {
            myCount.decrementAndGet();
        }
        return isRemoved;
    }

    private void resize()
    {
        Thread currentThread = Thread.currentThread();
        if (myOwner.compareAndSet(null, currentThread, false, true)) {
            // if you are able to acquire ownership for resizing then
            // proceed.

            int oldSize = myData.size();
            int oldLockSize = myLocks.size();

            // Spin till we acquire all the locks.
            for (ReentrantLock lock : myLocks) {
                while (lock.isLocked()) {
                }
            }

            int newSize = oldSize * 2;
            int newLockSize = oldLockSize * 2;

            List<ReentrantLock> newLocks = new ArrayList<>();
            for (int i = 0; i < newLockSize; i++) {
                newLocks.add(new ReentrantLock());
            }

            List<ListMultimap<K, V>> newData = new ArrayList<>();
            for (int i = 0; i < newSize; i++) {
                newData.add(LinkedListMultimap.<K, V>create());
            }

            for (int i = 0; i < oldSize; i++) {
                ListMultimap<K, V> dataMap = myData.get(i);
                for (Map.Entry<K, V> entry : dataMap.entries()) {
                    int newPos = entry.getKey().hashCode() % newSize;
                    newData.get(newPos).put(entry.getKey(), entry.getValue());
                }
            }

            myData = newData;
            myLocks = newLocks;
            myOwner.set(null, false);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long count()
    {
        return myCount.get();
    }
}
