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
import java.util.List;
import java.util.SortedSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Defines the contract for domain that holds a set of it's sorted elements
 *
 * @author Balraja Subbiah
 */
public class SetDomain<T extends Comparable<T>>
    implements DiscreteDomain<T>
{
    private List<T> myElements;

    /**
     * CTOR
     */
    public SetDomain()
    {
        myElements = null;
    }

    /**
     * CTOR
     */
    public SetDomain(SortedSet<T> elements)
    {
        myElements = Lists.newArrayList(elements.iterator());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T elementAt(long index)
    {
        Preconditions.checkArgument(
            index < myElements.size(),
            "The given index " + index
            + "exceeds the number of elements "
            + myElements.size());

        return myElements.get((int) index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getMaximum()
    {
        return myElements.get(myElements.size() - 1);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getMinimum()
    {
        return myElements.get(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalElements()
    {
        return myElements.size();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myElements = (List<T>) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myElements);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getMiddleOf(T lowerValue, T upperValue)
    {
        int lowerIndex = myElements.indexOf(lowerValue);
        int upperIndex = myElements.indexOf(upperValue);
        int middle = lowerIndex + (upperIndex - lowerIndex / 2);
        return myElements.get(middle);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T getMiddleOf(Object lowerValue, Object upperValue)
    {
        return getMiddleOf((T) lowerValue, (T) upperValue);
    }
}
