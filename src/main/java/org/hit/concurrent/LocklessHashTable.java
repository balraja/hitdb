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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Defines the contract for a hash table without any locks. It works based on
 * concepts of recursive split ordering as specified in the Art of Multiprocessor
 * Programming by moving the buckets via lockless sorted list.
 * 
 * @author Balraja Subbiah
 */
public class LocklessHashTable<K,V> implements HashTable<K, V>
{
    private static class BucketList<K,V>
        extends LocklessSortedList<HashTableNode<K, V>>
    {
        private final List<Node<HashTableNode<K, V>>> myIndexedBuckets;
        
        /**
         * The size of the hash table.
         */
        private final AtomicInteger mySize;
        
        /**
         * The number of buckets.
         */
        private final AtomicInteger myBucketSize;

        /**
         * CTOR
         */
        public BucketList()
        {
            super();
            myIndexedBuckets = new ArrayList<>();
            mySize = new AtomicInteger(0);
            myBucketSize = new AtomicInteger(2);
        }
        
        /**
         * Adds an entry to the list close to the bucket wrt key.hashCode()
         * % myBucketSize.get()
         */
        public boolean add(K key, V value)
        {
            int bucketIndex = key.hashCode() % myBucketSize.get();
            Node<HashTableNode<K, V>> indexNodeInList =
               myIndexedBuckets.get(bucketIndex);
            if (indexNodeInList == null) {
                HashTableNode<K, V> indexNode = new HashTableNode<>(bucketIndex);
                indexNodeInList = new Node<HashTableNode<K, V>>(indexNode);
                if (!add(indexNodeInList)) {
                    // Someone else has added the node before us.
                    indexNodeInList = myIndexedBuckets.get(bucketIndex);
                    
                }
                else {
                    if (myIndexedBuckets.get(bucketIndex) != null) {
                        myIndexedBuckets.add(bucketIndex, indexNodeInList);
                    }
                }
            }
            boolean result =
               add(new Node<HashTableNode<K, V>>(
                       new HashTableNode<K, V>(key, value)));
            
            if (result) {
                int size = mySize.incrementAndGet();
                int bucketSize = myBucketSize.get();
                int noItemsPerBucket = size / bucketSize;
                if (noItemsPerBucket > THRESHOLD) {
                    myBucketSize.compareAndSet(bucketSize,
                                               2 * bucketSize);
                }
            }
            return result;
        }
        
        /**
         * Looks up for the given key in the list.
         */
        public V lookup(K key)
        {
           
            int bucketIndex = key.hashCode() % myBucketSize.get();
            Node<HashTableNode<K, V>> indexNodeInList =
               myIndexedBuckets.get(bucketIndex);
            if (indexNodeInList == null) {
                return null;
            }
            else {
                HashTableNode<K, V> templateNode =
                    new HashTableNode<K, V>(key, null);
                HashTableNode<K, V> actualNode =
                    lookup(templateNode, indexNodeInList);
                return actualNode != null ? actualNode.getValue()
                                          : null;
            }
        }
    }
    
    /**
     * Defines the contract for a node that stores the given key and values
     * under the agegis of the bit recersed hash value of the key. When we
     * want to look for a key we have to search through the list using the
     * bit reversed hash value.
     */
    private static class HashTableNode<K,V>
        implements Comparable<HashTableNode<K,V>>
    {
        private final K myKey;
        
        private final V myValue;
        
        private final int myReversedHashKey;
        
        private final boolean mySentinal;
        
        /**
         * CTOR
         */
        public HashTableNode(int bucketIndex)
        {
            myReversedHashKey = Integer.reverse(bucketIndex);
            mySentinal = false;
            myKey = null;
            myValue = null;
        }
        
        /**
         * CTOR
         */
        public HashTableNode(K key, V value)
        {
            super();
            myKey = key;
            myValue = value;
            myReversedHashKey = Integer.reverse(key.hashCode());
            mySentinal = false;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public int compareTo(HashTableNode<K,V> o)
        {
            return myReversedHashKey < o.getReversedHashKey() ?
                       -1 : myReversedHashKey > o.getReversedHashKey() ? 1 : 0;
        }

        /**
         * Returns the value of key
         */
        public K getKey()
        {
            return myKey;
        }

        /**
         * Returns the value of reversedHashKey
         */
        public int getReversedHashKey()
        {
            return myReversedHashKey;
        }

        /**
         * Returns the value of value
         */
        public V getValue()
        {
            return myValue;
        }

        /**
         * Returns the value of sentinal
         */
        public boolean isSentinal()
        {
            return mySentinal;
        }
    }
    
    /**
     * The list which stores the data in a hash list.
     */
    private final BucketList<K, V> myList;
    
    /**
     * CTOR
     */
    public LocklessHashTable()
    {
        myList = new BucketList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean add(K key, V value)
    {
        return myList.add(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<V> get(K key)
    {
        // XXX Fix this.
        return Collections.singletonList(myList.lookup(key));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<V> getAllValues()
    {
        // TODO Fix this
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean remove(K key, V value)
    {
        // TODO Auto-generated method stub
        return false;
    }
}
