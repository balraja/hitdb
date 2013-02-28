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

import java.util.concurrent.atomic.AtomicMarkableReference;

import org.hit.util.Pair;

/**
 * Implementation of list like data structure without any locks. This is
 * based on the reference implementation from the book "Art of multiprocessor
 * programming".
 *
 * @author Balraja Subbiah
 */
public class LocklessSortedList<T extends Comparable<? super T>>
{
    /** Abstraction of a linked list node */
    protected static class Node<T>
    {
        private final T myData;

        private final AtomicMarkableReference<Node<T>> myNext;

        /**
         * CTOR
         */
        public Node(T data)
        {
            this(data, new AtomicMarkableReference<Node<T>>(null, false));
        }

        /**
         * CTOR
         */
        public Node(T data, AtomicMarkableReference<Node<T>> next)
        {
            super();
            myData = data;
            myNext = next;
        }

        /**
         * Returns the value of data
         */
        public T getData()
        {
            return myData;
        }

        /**
         * Returns the value of next
         */
        public AtomicMarkableReference<Node<T>> getNext()
        {
            return myNext;
        }
    }

    /** Refers to the head of linked list */
    private final Node<T> myHead;

    /** CTOR */
    public LocklessSortedList()
    {
        myHead = new Node<T>(null,
                             new AtomicMarkableReference<Node<T>>(null, false));
    }

    /**
     * Adds a given value to the list
     *
     * @param dataNode The dataNode to be added to the list
     * @return Returns true if the value is successfully added to the list,
     *         false if the value is already present.
     */
    protected boolean add(Node<T> dataNode)
    {
        T value = dataNode.getData();
        Pair<Node<T>, Node<T>> position = find(value);
        if (position.getSecond() == null) {
            position.getFirst()
                    .getNext()
                    .compareAndSet(null, dataNode, false, false);
            return true;
        }
        else {
            if (   position.getSecond() != null
                && position.getSecond().getData().compareTo(value) == 0)
            {
                return false;
            }

            boolean insertedIntoTheList =
                dataNode.getNext()
                        .compareAndSet(null, position.getSecond(), false, false);

            boolean listPointedToNewNode =
                position.getFirst()
                        .getNext()
                        .compareAndSet(position.getSecond(),
                                       dataNode,
                                       false,
                                       false);

            return insertedIntoTheList && listPointedToNewNode;
        }
    }

    /**
     * Adds a given value to the list
     *
     * @param value The value to be added to the list
     * @return Returns true if the value is successfully added to the list,
     *         false if the value is already present.
     */
    public boolean add(T value)
    {
        return add(new Node<T>(value,
                               new AtomicMarkableReference<Node<T>>(null, false)
        ));
    }

    /**
     * Returns true if the value is present in the list, false otherwise.
     */
    public boolean contains(T value)
    {
        Pair<Node<T>, Node<T>> position = find(value);
        return (   position.getSecond() != null
                && position.getSecond().getData().compareTo(value) == 0);
    }

    /**
     * A helper method to iterate through the sorted list and finds the
     * pair of nodes between which a value is to be inserted. While traversing
     * the list it cleans the marked nodes (i.e. those that are logically
     * deleted).
     */
    private Pair<Node<T>, Node<T>> find(T value)
    {
        return find(value, myHead);
    }

    /**
     * A helper method to iterate through the sorted list and finds the
     * pair of nodes between which a value is to be inserted. While traversing
     * the list it cleans the marked nodes (i.e. those that are logically
     * deleted).
     *
     * @param value The value searched for.
     * @param pred  The node from which we will start out searching.
     */
    private Pair<Node<T>, Node<T>> find(T value, Node<T> pred)
    {
        Node<T> curr = pred.getNext().getReference();
        if (curr == null) {
            return new Pair<>(pred, curr);
        }
        boolean[] marked = {false};
        while (true) {
            Node<T> succ = curr.getNext() != null ? curr.getNext().get(marked)
                                                  : null;
            while (marked[0]) {
                // Succ is marked for removal, so try removing it.
                boolean snip =
                    curr.getNext().compareAndSet(succ,
                                                 succ.getNext().getReference(),
                                                 true,
                                                 succ.getNext().isMarked());
                if (!snip) {
                    // If we couldn't change the reference try again.
                    curr = pred.getNext().getReference();
                    marked = new boolean[] {false};
                    succ = curr.getNext().get(marked);
                }
            }
            if (curr.getData().compareTo(value) < 0) {
                pred = curr;
                curr = curr.getNext().get(marked);
                if (curr == null) {
                    return new Pair<>(pred, curr);
                }
            }
            else {
                return new Pair<>(pred, curr);
            }
        }
    }

    /** Returns true if the list is empty, false otherwise */
    public boolean isEmpty()
    {
        return myHead.getNext().getReference() == null;
    }

    /**
     * Returns true if the value is present in the list, false otherwise.
     */
    protected T lookup(T value, Node<T> startNodeForSearch)
    {
        Pair<Node<T>, Node<T>> position = find(value, startNodeForSearch);
        if ( position.getSecond() != null
             && position.getSecond().getData().compareTo(value) == 0)
        {
            return position.getSecond().getData();
        }
        else {
            return null;
        }
    }

    /**
     * Removes the given value from the list.
     *
     * @param value The value to be removed.
     * @return Returns true if the value is present and removed, false otherwise.
     */
    public boolean remove(T value)
    {
        Pair<Node<T>, Node<T>> position = find(value);
        if (position.getSecond() == null) {
            return false;
        }
        else {
            if (   position.getSecond() != null
                && position.getSecond().getData().compareTo(value) == 0)
            {
                return position.getFirst().getNext().attemptMark(
                    position.getSecond(), false);
            }
            else {
                return false;
            }
        }
    }
}
