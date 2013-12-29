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

package org.hit.util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;

/**
 * Defines the contract for a class that implements the closed range.
 * 
 * @author Balraja Subbiah
 */
public class Range<T extends Comparable<T>> implements Externalizable
{
    private T myMinValue;
    
    private T myMaxValue;
    
    /**
     * CTOR
     */
    public Range()
    {
        super();
        myMinValue = null;
        myMaxValue = null;
    }

    /**
     * CTOR
     */
    public Range(T minValue, T maxValue)
    {
        super();
        myMinValue = minValue;
        myMaxValue = maxValue;
    }

    /**
     * Returns the value of minValue
     */
    public T getMinValue()
    {
        return myMinValue;
    }

    /**
     * Returns the value of maxValue
     */
    public T getMaxValue()
    {
        return myMaxValue;
    }
    
    /**
     * Returns true if this range contains the other range.
     */
    public boolean contains(Range<T> other)
    {
        return myMinValue.compareTo(other.getMinValue()) <= 0
            && myMaxValue.compareTo(other.getMaxValue()) >= 0;
    }
    
    /**
     * Returns true if this range overlaps the other range on 
     * it's left.
     */
    public boolean leftOverlap(Range<T> other)
    {
        return other.getMinValue().compareTo(myMaxValue) <= 0
            && myMaxValue.compareTo(other.getMaxValue()) <= 0;
    }
    
    /**
     * Returns true if this range overlaps the other range on 
     * it's right.
     */
    public boolean rightOverlap(Range<T> other)
    {
        return myMinValue.compareTo(other.getMaxValue()) <= 0
            && myMaxValue.compareTo(other.getMaxValue()) >= 0;
    }
    
    /**
     * A simple helper method for splitting the given range across 
     * multiple node ranges.
     */
    public List<Range<T>> split(List<Range<T>> nodeRanges)
    {
        ArrayList<Range<T>> partitionedRanges = new ArrayList<>();
        for (Range<T> nodeRange : nodeRanges) {
            if (contains(nodeRange)) {
                partitionedRanges.add(nodeRange);
            }
            else if (leftOverlap(nodeRange)) {
                partitionedRanges.add(
                    new Range<T>(nodeRange.getMinValue(), 
                                 myMaxValue));
            }
            else if (rightOverlap(nodeRange)) {
                partitionedRanges.add(
                    new Range<T>(myMinValue, nodeRange.getMaxValue()));
            }
        }
        return partitionedRanges;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myMinValue);
        out.writeObject(myMaxValue);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myMinValue = (T) in.readObject();
        myMaxValue = (T) in.readObject();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "[ " + myMinValue + " - " + myMaxValue + " ]";
    }
}
