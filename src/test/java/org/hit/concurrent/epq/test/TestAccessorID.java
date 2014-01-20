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
package org.hit.concurrent.epq.test;

import org.hit.concurrent.epq.AccessorID;
import org.hit.concurrent.epq.WaitStrategy;

/**
 * Implements {@link AccessorID} that can be used for tests.
 * 
 * @author Balraja Subbiah
 */
public class TestAccessorID implements AccessorID
{
    private final WaitStrategy myWaitStrategy;
    
    private final String myName;
    
    /**
     * CTOR
     */
    public TestAccessorID(String name, WaitStrategy strategy)
    {
        myName = name;
        myWaitStrategy = strategy;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((myName == null) ? 0 : myName.hashCode());
        result = prime * result
                + ((myWaitStrategy == null) ? 0 : myWaitStrategy.hashCode());
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
        TestAccessorID other = (TestAccessorID) obj;
        if (myName == null) {
            if (other.myName != null)
                return false;
        }
        else if (!myName.equals(other.myName))
            return false;
        if (myWaitStrategy != other.myWaitStrategy)
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "TestAccessorID [myWaitStrategy=" + myWaitStrategy + ", myName="
                + myName + "]";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WaitStrategy getWaitStrategy()
    {
        return myWaitStrategy;
    }
}
