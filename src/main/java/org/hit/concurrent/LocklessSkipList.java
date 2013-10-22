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
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicMarkableReference;

import com.google.common.collect.Lists;

/**
 * Defines a skip list built without using any locks. The design is based on the
 * lockless algorithm explained in the Art of Multiprocessor Programming. One
 * important aspect to note is that the skip list property will not be
 * maintained always. An element is considered to be in the set if it's
 * contained in the bottom level list and that defines the linearization point
 * for add.
 *
 * @author Balraja Subbiah
 */
public class LocklessSkipList<K extends Comparable<? super K>,V>
{
    /**
     * Defines the structure of a node in a skip list
     */
    public static class Node<K extends Comparable<? super K>,V>
    {
        private final K myKey;

        private final int myLevel;

        private final List<AtomicMarkableReference<Node<K,V>>> myNext;

        private final List<V> myValues;

        /**
         * CTOR
         */
        public Node(K key, V value, int level, int listLevel)
        {
            super();
            myKey = key;
            myValues = new CopyOnWriteArrayList<>();
            myValues.add(value);
            myLevel = level;
            myNext  = new ArrayList<>();
            for (int i = 0; i < listLevel; i++) {
                myNext.add(
                    new AtomicMarkableReference<Node<K,V>>(null, false));
            }
        }

        /**
         * Returns the value of key
         */
        public K getKey()
        {
            return myKey;
        }

        /**
         * Returns the value of level
         */
        public int getLevel()
        {
            return myLevel;
        }

        /**
         * Returns the value of next
         */
        public List<AtomicMarkableReference<Node<K, V>>> getNext()
        {
            return myNext;
        }

        /**
         * Returns the value of value
         */
        public List<V> getValues()
        {
            return myValues;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString()
        {
            return "Node [myKey=" + myKey
                   + ", myLevel="
                   + myLevel
                   + ", myValues="
                   + myValues
                   + "]";
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((myKey == null) ? 0 : myKey.hashCode());
            result = prime * result + myLevel;
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Node other = (Node) obj;
            if (myKey == null) {
                if (other.myKey != null)
                    return false;
            }
            else if (!myKey.equals(other.myKey))
                return false;
            if (myLevel != other.myLevel)
                return false;
            return true;
        }
        
        
    }

    /**
     * Implements <code>Iterator</code> to iterate over elements of the
     * skip list.
     */
    public class SkipListIterator implements Iterator<List<V>>
    {
        private Node<K, V> myNode;

        /**
         * CTOR
         */
        public SkipListIterator(Node<K, V> node)
        {
            myNode = node;
        }

        protected Node<K,V> getMyNode()
        {
            return myNode;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            return myNode != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<V> next()
        {
            List<V> result = myNode.getValues();

            Node<K,V> pred = myNode;
            Node<K,V> curr = null;
            while (true) {
                curr = pred.getNext().get(0).getReference();
                // if the reference is marked just skip through it.
                if (pred != null && pred.getNext().get(0).isMarked()) {
                    pred = curr;
                    continue;
                }
                else {
                    break;
                }
            }
            myNode = curr;
            return result;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void remove()
        {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Implements <code>Iterator</code> to iterate over elements of the
     * skip list.
     */
    public class SkipListRangeIterator extends SkipListIterator
    {
        private final K myEndValue;

        /**
         * CTOR
         */
        public SkipListRangeIterator(K startValue, K endValue)
        {
            super(lookupNode(startValue));
            myEndValue = endValue;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            return getMyNode() != null
                && getMyNode().getKey().compareTo(myEndValue) <= 0;
        }
    }

    private final Node<K,V> myHead;

    /** Defines the level of the skip list */
    private final int myListLevel;

    private final ThreadLocal<Random> myLocalRandom;
    
    private final AtomicLong myCount;

    /**
     * CTOR
     */
    public LocklessSkipList(int listLevel)
    {
        super();
        myListLevel = listLevel;
        myHead = new Node<>(null, null, myListLevel, myListLevel);
        myLocalRandom = new ThreadLocal<Random>() {
            @Override
            protected Random initialValue() {
                return new Random();
            }
        };
        myCount = new AtomicLong(0L);
    }

    /**
     * Adds a given key value pair to the skip list.
     *
     * @param key The key to be added to the skip list.
     * @param value The value to be added to the skip list
     * @return true if the addition is successful, false otherwise.
     */
    public void add(K key, V value)
    {
        List<Node<K,V>> preds = 
            Lists.newArrayListWithExpectedSize(myListLevel);
        List<Node<K,V>> succs = 
            Lists.newArrayListWithExpectedSize(myListLevel);

        for (int i = 0; i < myListLevel; i++) {
            preds.add(null);
            succs.add(null);
        }
        boolean isThere = find(key, preds, succs);
        if (isThere) {
           succs.get(0).getValues().add(value);
           myCount.incrementAndGet();
        }
        else {
            
            boolean myShouldFindAgain = false;
            Node<K,V> newNode = null;
            do {
                if (myShouldFindAgain) {
                    find(key, preds, succs);
                    myShouldFindAgain = false;
                    newNode = null;
                }
                
                int nodeLevel = 
                    Math.abs(myLocalRandom.get().nextInt()) % myListLevel;
                newNode = new Node<>(key, value, nodeLevel, myListLevel);
                for (int i = 0; i < nodeLevel; i++) {
                    newNode.getNext()
                           .get(i)
                           .compareAndSet(null, succs.get(i), false, false);
                }
    
                if (!preds.get(0)
                          .getNext()
                          .get(0)
                          .compareAndSet(succs.get(0), newNode, false, false))
                {
                    myShouldFindAgain = true;
                }
                else {
                    for (int i = 1; i < nodeLevel; i++) {
                        if (!preds.get(i)
                                  .getNext()
                                  .get(i)
                                  .compareAndSet(succs.get(i),
                                                 newNode,
                                                 false,
                                                 false))
                        {
                            for (int j = 1; j < i; j++) {
                                preds.get(i).getNext().get(i).set(succs.get(i),
                                                                  false);
                            }
                            myShouldFindAgain = true;
                            break;
                        }
                    }
                }
            } while(myShouldFindAgain);
            
        }
        myCount.incrementAndGet();
    }

    /**
     * A helper method to find predecessors and successors for a given key
     * at various levels of the skip list.
     */
    private boolean find(K key, List<Node<K,V>> preds, List<Node<K,V>> succs)
    {
        Node<K,V> pred = myHead;
        for (int level = myListLevel - 1; level >= 0; level--) {
            boolean[] mark = new boolean[]{false};
            while (true) {
                Node<K,V> curr = pred.getNext().get(level).getReference();
                Node<K,V> succ =
                        curr != null ? curr.getNext().get(level).get(mark)
                                     : null;
                if (mark[0]) {
                    curr.getNext()
                        .get(level)
                        .compareAndSet(succ,
                                       succ.getNext().get(level).getReference(),
                                       true,
                                       succ.getNext().get(level).isMarked());
                    continue;
                }

                if (curr != null && curr.getKey().compareTo(key) < 0) {
                    pred = curr;
                    curr = curr.getNext().get(level).getReference();
                }
                else {
                    preds.set(level, pred);
                    succs.set(level, curr);
                    break;
                }
            }

        }
        return succs.get(0) != null && succs.get(0).getKey().compareTo(key) == 0;
    }

    /**
     * Returns an <code>Iterator</code> over all values in this skip list
     */
    public SkipListIterator lookupAllValues()
    {
        return new SkipListIterator(lookupFirstNode());
    }

    /**
     * Returns the first node in the skip list.
     */
    private Node<K,V> lookupFirstNode()
    {
        Node<K,V> pred = myHead;
        Node<K,V> curr = null;
        while (true) {
            curr = pred.getNext().get(0).getReference();
            // if the reference is marked just skip through it.
            if (pred != null && pred.getNext().get(0).isMarked()) {
                pred = curr;
                continue;
            }
            else {
                break;
            }
        }
        return curr;
    }

    /**
     * Returns node less than or equal to the given key.
     */
    private Node<K,V> lookupNode(K key)
    {
        //System.out.println(Thread.currentThread().getName() + " LOOKING FOR " + key);
        Node<K,V> pred = myHead;
        Node<K,V> curr = null;
        for (int level = myListLevel - 1; level >= 0; level--) {
           // System.out.println(Thread.currentThread().getName() + " EXAMING LIST " + level);
            while (true) {
                curr = pred.getNext().get(level).getReference();
                // if the reference is marked just skip through it.
                if (pred != null && pred.getNext().get(0).isMarked()) {
                    pred = curr;
                    continue;
                }

                if (curr != null) {
                    //System.out.println(Thread.currentThread().getName()
                   //                    + " Examining value " + curr.myKey);

                    if (curr.getKey().compareTo(key) < 0) {
                        // Current value is less than the searched key
                        // continue searching in this level.
                        pred = curr;
                    }
                    else if (curr.getKey().compareTo(key) > 0) {
                        // current value is greater than the searched value
                        // hence move to the next level
                        break;
                    }
                    else if (curr.getKey().compareTo(key) == 0) {
                        return curr;
                    }
                }
                else {
                    //System.out.println(Thread.currentThread().getName() + " CURR IS NULL");
                    // THis refers to the case where there are no matching nodes
                    // at the given level
                    break;
                }
            }

           // System.out.println(Thread.currentThread().getName() + " BREAKING OUT OF LEVEL " + level);
        }
        return curr;
    }

    /**
     * Returns the value corresponding to the given key if it's present, null
     * otherwise.
     */
    public List<V> lookupValue(K key)
    {
        Node<K,V> curr = lookupNode(key);
        return (curr != null && curr.getKey().compareTo(key) == 0) ?
                   curr.getValues() : null;
    }

    /**
     * Returns an <code>Iterator</code> over values in the given range
     * specified by the start and end values.
     */
    public SkipListIterator lookupValues(K start, K end)
    {
        return new SkipListRangeIterator(start, end);
    }

    /**
     * Removes the given key value pair from the skip list.
     *
     * @param key The key whose value is to be removed from the skip list.
     * @param value The value that has to be removed from the skip list.
     * @return true if the removal is successful, false otherwise.
     */
    public boolean remove(K key, V value)
    {
        List<Node<K,V>> preds = new ArrayList<>(myListLevel);
        List<Node<K,V>> succs = new ArrayList<>(myListLevel);
        boolean isThere = find(key, preds, succs);
        if (!isThere) {
            return false;
        }
        else {
            Node<K, V> nodeToBeRemoved = succs.get(0);
            nodeToBeRemoved.getValues().remove(value);
            if (nodeToBeRemoved.getValues().isEmpty()) {
                for (int i = 1; i < nodeToBeRemoved.getLevel(); i++) {
                    boolean[] marked = new boolean[] {false};
                    Node<K, V> succ =
                            nodeToBeRemoved.getNext()
                                           .get(i)
                                           .get(marked);
                    while (!marked[0]) {
                        nodeToBeRemoved.getNext()
                                       .get(i)
                                       .attemptMark(succ, true);
                        succ =
                                nodeToBeRemoved.getNext()
                                               .get(i)
                                               .get(marked);
                    }
                }

                while (true) {
                    boolean[] marked = new boolean[] {false};
                    Node<K, V> succ =
                            nodeToBeRemoved.getNext()
                                           .get(0)
                                           .get(marked);

                    if (nodeToBeRemoved.getNext()
                                        .get(0)
                                        .compareAndSet(succ, succ, false, true))
                    {
                        myCount.decrementAndGet();
                        return true;
                    }
                    else if (marked[0]) {
                        return false;
                    }
                }
            }
            else {
                myCount.decrementAndGet();
                return true;
            }
        }
    }

    /**
     * Returns the value of count
     */
    public long getCount()
    {
        return myCount.get();
    }
}
