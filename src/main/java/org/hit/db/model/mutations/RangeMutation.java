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

package org.hit.db.model.mutations;

import org.hit.db.model.Mutation;
import org.hit.util.Range;

/**
 * Defines the contract for a class that can be used for applying
 * {@link Mutation} for a range of keys.
 * 
 * @author Balraja Subbiah
 */
public interface RangeMutation<K extends Comparable<K>> 
    extends Mutation, Cloneable
{
    /** 
     * Defines the contract for the range of keys for which this mutation
     * is to be applied.
     */
    public Range<K> getKeyRange();
}
