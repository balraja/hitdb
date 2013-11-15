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

package org.hit.db.keyspace.domain;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.google.common.base.Preconditions;

/**
 * Defines the domain of consequtive integers between start and end.
 *
 * @author Balraja Subbiah
 */
public class IntegerDomain implements DiscreteDomain<Integer>
{
    private int myEnd;

    private int myStart;

    /**
     * CTOR
     */
    public IntegerDomain()
    {
        myStart = -1;
        myEnd   = -1;
    }

    /**
     * CTOR
     */
    public IntegerDomain(int start, int end)
    {
        super();
        myStart = start;
        myEnd = end;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer elementAt(long index)
    {
        Preconditions.checkArgument(myStart + index <= myEnd);
        return Integer.valueOf((int) (myStart + index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getMaximum()
    {
        return Integer.valueOf(myEnd);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getMinimum()
    {
        return Integer.valueOf(myStart);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalElements()
    {
        return (myEnd - myStart) + 1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myStart = in.readInt();
        myEnd = in.readInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(myStart);
        out.writeInt(myEnd);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getMiddleOf(Integer lowerValue, Integer upperValue)
    {
        return Integer.valueOf(lowerValue + (upperValue - lowerValue / 2));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Integer getMiddleOf(Object lowerValue, Object upperValue)
    {
        return getMiddleOf((Integer) lowerValue, (Integer) upperValue);
    }
}
