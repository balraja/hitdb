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
package org.hit.concurrent;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Implements a bounded {@link Stack} which has predefined size and it 
 * supports lockless push and pop. 
 * 
 * @author Balraja Subbiah
 */
public class UnBoundedLocklessStack<T>
{
    private final AtomicReference<Node<T>> myStackHead;
    
    /**
     * CTOR
     */
    public UnBoundedLocklessStack(int stackSize)
    {
        myStackHead = new AtomicReference<Node<T>>(null);
    }
    
    /**
     * Pushes the elements into the queue.
     */
    public void push(T element)    
    {
        Node<T> elementNode = 
            new Node<T>(element, 
                        new AtomicReference<Node<T>>(myStackHead.get()));
        Node<T> head = myStackHead.get();
        while (true) {
            if (myStackHead.compareAndSet(head, elementNode)) {
                return;
            }
            else {
                head = myStackHead.get();
                elementNode.getNext().set(head);
            }
        }
    }
    
    /**
     * Pops an element out of the stack.
     */
    public T pop()
    {
        Node<T> head = myStackHead.get();
        Node<T> next = head.getNext().get();
        while (true) {
           
            if (myStackHead.compareAndSet(head, next)) {
                return head.getData();
            }
            else {
                head = myStackHead.get();
                next = head.getNext().get();
            }
        }
    }
}
