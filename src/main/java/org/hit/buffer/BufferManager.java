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
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.util.LogFactory;

/**
 * This class preallocates memory outside jvm's heap and manages that memory.
 * 
 * @author Balraja Subbiah
 */
public class BufferManager
{ 
    public static final int BUFFER_SIZE = 1024;
    
    private static final Logger LOG =
         LogFactory.getInstance().getLogger(BufferManager.class);
    
    private final List<ByteBuffer> myBuffers;
    
    private final Lock myBufferPoolLock;
    
    private final Condition myAwaitingFreeSpaceCondition;
    
    /**
     * CTOR
     */
    public BufferManager(String namespace, BufferConfig config)
    {
        this(config.getBufferSize(namespace));
        LOG.info("For Namespace " + namespace);
    }

    /**
     * CTOR
     */
    public BufferManager(int numBuffers)
    {
        myBuffers = new LinkedList<>();
        for (int i = 1; i <= numBuffers; i++) {
            myBuffers.add(ByteBuffer.allocateDirect(BUFFER_SIZE));
        }
        LOG.info("Initialized buffer with " + myBuffers.size() + " blocks ");
        myBufferPoolLock = new ReentrantLock();
        myAwaitingFreeSpaceCondition = myBufferPoolLock.newCondition();
    }
    
    /**
     * Returns a chunk of preallocated {@link ByteBuffer} of size 1KB.
     */
    public ByteBuffer getBuffer()
    {
        try {
            myBufferPoolLock.lock();
            if (myBuffers.isEmpty()) {
                myAwaitingFreeSpaceCondition.await();
            }
            else {
                ByteBuffer buffer = myBuffers.remove(0);
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("ALLOC : Current size of buffer pool " + myBuffers.size());
                }
                return buffer;
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
    public List<ByteBuffer> getBuffer(int bufferSize)
    {
        try {
            int maxBuffers = bufferSize / BUFFER_SIZE;
            if (bufferSize % BUFFER_SIZE > 0) {
                maxBuffers++;
            }
            myBufferPoolLock.lock();
            if (myBuffers.isEmpty()) {
                myAwaitingFreeSpaceCondition.await();
            }
            else if (myBuffers.size() < maxBuffers) {
                myAwaitingFreeSpaceCondition.await();
            }
            else {
                List<ByteBuffer> removedValues = new ArrayList<>();
                for (int i = 0; i < maxBuffers; i++) {
                    removedValues.add(myBuffers.remove(0));
                }
                if (LOG.isLoggable(Level.FINEST)) {
                    LOG.finest("ALLOC : Current size of buffer pool " + myBuffers.size());
                }
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
            buffer.clear();
            myBuffers.add(buffer);
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("Current size of buffer pool " + myBuffers.size());
            }
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
            for (ByteBuffer buffer : buffers) {
                buffer.clear();
                myBuffers.add(buffer);
            }
            buffers.clear();
            if (LOG.isLoggable(Level.FINEST)) {
                LOG.finest("FREE Current size of buffer pool" + myBuffers.size());
            }
            myAwaitingFreeSpaceCondition.signal();
        }
        finally {
            myBufferPoolLock.unlock();
        }
    }
}
