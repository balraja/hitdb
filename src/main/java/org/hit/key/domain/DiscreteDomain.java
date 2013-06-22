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

import java.io.Externalizable;

/**
 * Defines the contract for a domain that defines the discrete countable
 * elements of a particual type.
 *
 * @author Balraja Subbiah
 */
public interface DiscreteDomain<T extends Comparable<T>> extends Externalizable
{
    /**
     * Returns the value at the position offset from the given value.
     */
    T elementAt(long index);

    /** Returns the maximum value of a domain */
    T getMaximum();

    /** Returns the minimum value of a domain */
    T getMinimum();

    /** Returns the total number of elements in a domain */
    long getTotalElements();
}
