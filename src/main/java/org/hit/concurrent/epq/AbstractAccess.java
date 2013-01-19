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

package org.hit.concurrent.epq;

/**
 * An abstract implementation of <code>Access</code> that implements the
 * waiting behaviour for various wait strategies.
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractAccess implements Access
{
    private final WaitStrategy myWaitStrategy;
    
    /**
     * CTOR
     */
    public AbstractAccess(WaitStrategy waitStrategy)
    {
        myWaitStrategy = waitStrategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void waitFor()
    {
        if (myWaitStrategy == WaitStrategy.SLEEP) {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
            }
        }
    }
}
