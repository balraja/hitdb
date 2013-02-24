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

package org.hit.distribution;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Implements a hash ring which spans over ZERO to Long.MAX_VALUE and then
 * wraps over.
 * 
 * @author Balraja Subbiah
 */
public class LongKeySpace implements KeySpace<Long>
{
    private long myMinValue;
    
    private long myMaxValue;
    
    /**
     * CTOR
     */
    public LongKeySpace(long minValue, long maxValue)
    {
        super();
        myMinValue = Math.abs(minValue);
        myMaxValue = Math.abs(maxValue);
        assert myMinValue > 0L;
        assert myMinValue < myMaxValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getMaximum()
    {
        return myMaxValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long getMinimum()
    {
        return myMinValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalElements()
    {
        return (myMaxValue - myMinValue) + 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long nextAtOffset(Long currentValue, long offset)
    {
        if (myMaxValue - currentValue < offset) {
            offset = offset - (myMaxValue - currentValue);
            return Long.valueOf(myMinValue + offset);
        }
        return Long.valueOf(currentValue + offset);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myMinValue = in.readLong();
        myMaxValue = in.readLong();
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
    public void setMinimum(Object min)
    {
        myMinValue = ((Long) min);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaximum(Object max)
    {
        myMaxValue = ((Long) max);
    }
}
