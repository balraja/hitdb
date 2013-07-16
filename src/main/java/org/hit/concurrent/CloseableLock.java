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
import java.util.concurrent.locks.Lock;

/**
 * Implements the lock with closeable interface so that it can be closed
 * after it's use automatically.
 *
 * @author Balraja Subbiah
 */
public class CloseableLock implements Closeable
{
    private final Lock myLock;

    /**
     * CTOR
     */
    public CloseableLock(Lock lock)
    {
        myLock = lock;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        myLock.unlock();
    }

    /**
     * This lock will be used for protecting the critical segments
     * encapsulated within the new style try blocks.
     */
    public CloseableLock open()
    {
        myLock.lock();
        return this;
    }
}
