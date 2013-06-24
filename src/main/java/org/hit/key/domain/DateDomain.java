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

package org.hit.key.domain;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Date;

/**
 * Defines the contract for a domain that defines all dates between the
 * specified start date and end date.
 *
 * @author Balraja Subbiah
 */
public class DateDomain implements DiscreteDomain<Date>
{
    private static final long ONE_DAY_IN_MILLIS = 24 * 60 * 60 * 1000;

    private long myEndMillis;

    private long myStartMillis;

    /**
     * CTOR
     */
    public DateDomain()
    {
        myStartMillis = -1;
        myEndMillis = -1;
    }

    /**
     * CTOR
     */
    public DateDomain(Date start, Date end)
    {
        // Move the time exactly to the start of a day.
        myStartMillis = start.getTime();
        myStartMillis -= (myStartMillis % ONE_DAY_IN_MILLIS);
        myEndMillis = end.getTime();
        myEndMillis -= (myStartMillis % ONE_DAY_IN_MILLIS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date elementAt(long index)
    {
        return new Date(myStartMillis + index * ONE_DAY_IN_MILLIS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getMaximum()
    {
        return new Date(myEndMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getMinimum()
    {
        return new Date(myStartMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalElements()
    {
        return ((myEndMillis - myStartMillis) / ONE_DAY_IN_MILLIS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
        myStartMillis = in.readLong();
        myEndMillis = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(myStartMillis);
        out.writeLong(myEndMillis);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getMiddleOf(Date lowerValue, Date upperValue)
    {
        long delta = (upperValue.getTime() - lowerValue.getTime()) << 1;
        return new Date(lowerValue.getTime() + delta);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getMiddleOf(Object lowerValue, Object upperValue)
    {
        return getMiddleOf((Date) lowerValue, (Date) upperValue);
    }
}
