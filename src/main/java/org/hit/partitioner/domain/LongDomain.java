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

package org.hit.partitioner.domain;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Defines the contract for a discrete domain containing long values
 * 
 * @author Balraja Subbiah
 */
public class LongDomain implements DiscreteDomain<Long>
{
    private long myMinValue;
    
    private long myMaxValue;
    
    /**
     * CTOR
     */
    public LongDomain()
    {
        myMinValue = -1L;
        myMaxValue = -1L;
    }
    
    /**
     * CTOR
     */
    public LongDomain(long minValue, long maxValue)
    {
        myMinValue = minValue;
        myMaxValue = maxValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(myMinValue);
        out.writeLong(myMaxValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,ClassNotFoundException
    {
        myMinValue = in.readLong();
        myMaxValue = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long elementAt(long index)
    {
        return Long.valueOf(myMinValue + index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getMaximum()
    {
        return Long.valueOf(myMaxValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getMinimum()
    {
        return Long.valueOf(myMinValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalElements()
    {
        return myMaxValue - myMinValue + 1;
    }
}
