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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A simple queue like data structure that's bounded in it's capacity. Also 
 * this queue is partial in the sense that enqueue and dequeue methods are 
 * blocking. This is also based on the ideas from the Art of multiprocessor 
 * programming.
 * 
 * @author Balraja Subbiah
 */
public class BoundedQueue<T>
{
    private final List<T> myData;
    
    private final AtomicInteger mySize;
    
    private final int myCapacity;
    
    private final Lock myEnqueueLock;
    
    private final Lock myDequeueLock;
    
    private final Condition myNoSpaceCondition;
    
    private final Condition myNoDataCondition;

    /**
     * CTOR
     */
    public BoundedQueue(int capacity)
    {
        myCapacity = capacity;
        myData = new ArrayList<>(capacity);
        mySize = new AtomicInteger(0);
        myEnqueueLock = new ReentrantLock();
        myDequeueLock = new ReentrantLock();
        myNoSpaceCondition = myEnqueueLock.newCondition();
        myNoDataCondition = myDequeueLock.newCondition();
    }
    
    /** Enqueues data to the end of queue */
    public void enqueue(T data)
    {
        try {
            myEnqueueLock.lock();
            if (mySize.get() == myCapacity) {
                myNoSpaceCondition.await();
            }
          
            myData.add(mySize.getAndIncrement(), data);
            myEnqueueLock.unlock();
            myDequeueLock.lock();
            myNoDataCondition.signalAll();
            myDequeueLock.unlock();
           
        }
        catch (InterruptedException e) {
        }
    }
    
    /** Reads data from the front of the queue */
    public T dequeue()
    {
        T data = null;
        try {
            myDequeueLock.lock();
            if (mySize.get() == 0) {
                myNoDataCondition.await();
            }
            
            data = myData.get(mySize.getAndDecrement() - 1);
            myDequeueLock.unlock();
            myEnqueueLock.lock();
            myNoSpaceCondition.signalAll();
            myEnqueueLock.unlock();
        }
        catch(InterruptedException e) {
        }
        return data;
    }
}
