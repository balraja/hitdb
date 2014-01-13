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
package org.hit.buffer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class preallocates memory outside jvm's heap and manages that memory.
 * 
 * @author Balraja Subbiah
 */
public class BufferManager
{ 
    private static final int SIZE = 1024;
    
    private final List<ByteBuffer> myBuffers;
    
    private final Lock myBufferPoolLock;
    
    private final Condition myAwaitingFreeSpaceCondition;
    
    /**
     * CTOR
     */
    public BufferManager(String namespace, BufferConfig config)
    {
        this(config.getBufferSize(namespace));
    }

    /**
     * CTOR
     */
    public BufferManager(int numBuffers)
    {
        myBuffers = new LinkedList<>();
        for (int i = 0; i < numBuffers; i++) {
            myBuffers.add(ByteBuffer.allocateDirect(SIZE));
        }
        myBufferPoolLock = new ReentrantLock();
        myAwaitingFreeSpaceCondition = myBufferPoolLock.newCondition();
    }
    
    /**
     * Returns a chunk of preallocated {@link ByteBuffer} of size 1KB.
     */
    public synchronized ByteBuffer getBuffer()
    {
        
        try {
            myBufferPoolLock.lock();
            if (myBuffers.isEmpty()) {
                myAwaitingFreeSpaceCondition.wait();
            }
            else {
                return myBuffers.remove(0);
            }
        }
        catch (InterruptedException e) {
            // Do nothing.
        }
        finally {
            myBufferPoolLock.unlock();
        }
        return null;
    }
    
    /**
     * Returns a chunks of preallocated {@link ByteBuffer} of size 1KB to 
     * match the expected size.
     */
    public synchronized List<ByteBuffer> getBuffer(int bufferSize)
    {
        try {
            int maxBuffers = bufferSize / SIZE;
            if (bufferSize % SIZE > 0) {
                maxBuffers++;
            }
            myBufferPoolLock.lock();
            if (myBuffers.isEmpty()) {
                myAwaitingFreeSpaceCondition.wait();
            }
            else if (myBuffers.size() < maxBuffers) {
                myAwaitingFreeSpaceCondition.await();
            }
            else {
                List<ByteBuffer> removedValues = 
                    myBuffers.subList(0, maxBuffers);
                myBuffers.removeAll(removedValues);
                return removedValues;
            }
        }
        catch (InterruptedException e) {
            // Do nothing.
        }
        finally {
            myBufferPoolLock.unlock();
        }
        return null;
    }
    
    /**
     * Returns the {@link ByteBuffer} back to the pool.
     */
    public void free(ByteBuffer buffer)
    {
        try {
            myBufferPoolLock.lock();
            myBuffers.add(buffer);
            myAwaitingFreeSpaceCondition.signal();
        }
        finally {
            myBufferPoolLock.unlock();
        }
    }
    
    /**
     * Returns the collection of {@link ByteBuffer}s back to the pool.
     */
    public void free(Collection<ByteBuffer> buffers)
    {
        try {
            myBufferPoolLock.lock();
            myBuffers.addAll(buffers);
            myAwaitingFreeSpaceCondition.signal();
        }
        finally {
            myBufferPoolLock.unlock();
        }
    }
}
