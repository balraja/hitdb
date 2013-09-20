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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A simple class to implement the <code>Closeable</code> interface for 
 * <code>ReadWriteLock</code>
 * 
 * @author Balraja Subbiah
 */
public class CloseableRWLock implements Closeable
{
    private final ReadWriteLock myReadWriteLock;
    
    private Lock myUsedLock;

    /**
     * CTOR
     */
    public CloseableRWLock(ReadWriteLock readWriteLock)
    {
        myReadWriteLock = readWriteLock;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() 
    {
        if (myUsedLock != null) {
            myUsedLock.unlock();
            myUsedLock = null;
        }
    }
    
    /** Acquires a read lock over a critical code */
    public CloseableRWLock openReadLock()
    {
        myUsedLock = myReadWriteLock.readLock();
        myUsedLock.lock();
        return this;
    }
    
    /** Acquires a write lock over a critical code */
    public CloseableRWLock openWriteLock()
    {
        myUsedLock = myReadWriteLock.writeLock();
        myUsedLock.lock();
        return this;
    }
}
