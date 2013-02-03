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

import java.io.Externalizable;

/**
 * Defines the contract for a closed hash ring where the maximum value is
 * followed by the minimum value and hence closes the ring.
 * 
 * @author Balraja Subbiah
 */
public interface KeySpace<T extends Comparable<? super T>>
    extends Externalizable
{
    /**
     * Returns the value closest to the current value in the HashRing
     */
    T ceiling(T currentValue);
    
    /** Returns the maximum value of the hash range */
    T getMaximum();
    
    /** Returns the minimum value of the hash range */
    T getMinimum();
    
    /** Returns the total number of elements in the hash ring */
    long getTotalElements();
    
    public void setMinimum(Object min);
    
    public void setMaximum(Object max);
    
    /**
     * Returns the value at the position offset from the given value.
     */
    T nextAtOffset(T currentValue, long offset);
}
