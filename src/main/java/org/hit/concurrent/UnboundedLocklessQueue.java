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

import java.util.concurrent.atomic.AtomicStampedReference;

/**
 * This defines a queue without any bounds. Also this queue doesn't use 
 * any locks.
 * 
 * @author Balraja Subbiah
 */
public class UnboundedLocklessQueue<T>
{
    private static final int REF_ARRAY_SIZE = 1;
    
    /** 
    * Defines the node that holds the data in a queue.
    */
   private static class Node<T>
   {
       private final T myData;
       
       private final AtomicStampedReference<Node<T>> myNext;

       /**
        * CTOR
        */
       public Node(T data, AtomicStampedReference<Node<T>> next)
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
       public AtomicStampedReference<Node<T>> getNext()
       {
           return myNext;
       }
   }
    
    /** Defines the exception thrown by the queue on error conditions */
    public static class DataException extends Exception
    {
        /**
         * The default serial version number.
         */
        private static final long serialVersionUID = 1L;

        /**
         * CTOR
         */
        public DataException()
        {
            super();
        }

        /**
         * CTOR
         */
        public DataException(String message, Throwable cause)
        {
            super(message, cause);
        }

        /**
         * CTOR
         */
        public DataException(String message)
        {
            super(message);
        }

        /**
         * CTOR
         */
        public DataException(Throwable cause)
        {
            super(cause);
        }
    }
    
    private final AtomicStampedReference<Node<T>> myHead;
    
    private final AtomicStampedReference<Node<T>> myTail;

    /**
     * CTOR
     */
    public UnboundedLocklessQueue()
    {
        super();
        myHead = new AtomicStampedReference<Node<T>>(null, 0);
        myTail = new AtomicStampedReference<Node<T>>(null, 0);
    }
    
    /**
     * Enqueues data to the end of queue. 
     * 
     * @param data            The data to be added to the queue
     * @throws DataException  The exception that gets thrown when the 
     *                        enqueue operation cannot be performed.
     */
    public void enqueue(T data) throws DataException
    {
        int[] firstStamp = new int[REF_ARRAY_SIZE];
        int[] lastStamp = new int[REF_ARRAY_SIZE];
        
        Node<T> first = myHead.get(firstStamp);
        Node<T> last = myTail.get(lastStamp);
        Node<T> dataNode = new Node<T>(data, null);
        if (last != null) {
            if (!last.getNext().compareAndSet(
                    null, dataNode, lastStamp[0], lastStamp[0] + 1)) 
            {
                throw new DataException("Unable to add data");
            }
        }
        if (!myTail.compareAndSet(last, dataNode, lastStamp[0], lastStamp[0] + 1)) 
        {
            throw new DataException("Unable to add data");
        }
        
        if (first == last && first == null) {
            myHead.compareAndSet(null, dataNode, firstStamp[0], firstStamp[0] + 1);
        }
    }
    
    /**
     * Dequeues data from the end of queue. 
     * 
     * @return data           Returns data from the top of queue.
     * @throws DataException  The exception that gets thrown when the 
     *                        enqueue operation cannot be performed.
     */
    public T dequeue() throws DataException
    {
        int[] firstStamp = new int[REF_ARRAY_SIZE];
        int[] lastStamp = new int[REF_ARRAY_SIZE];
        int[] nextStamp = new int[REF_ARRAY_SIZE];
        
        Node<T> first = myHead.get(firstStamp);
        Node<T> last = myTail.get(lastStamp);
        Node<T> next = first != null ? first.getNext().get(nextStamp) : null;
        if (first == null) {
            throw new DataException("Queue is empty");
        }
        T data = first.getData();
        if (!myHead.compareAndSet(first, next, firstStamp[0], firstStamp[0] + 1)) 
        {
            throw new DataException("Unable to dequeue data");
        }
        if (first == last && next != null) {
            myTail.compareAndSet(last, next, lastStamp[0], lastStamp[0] + 1);
        }
        return data;
    }
}
