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

package org.hit.key;

import java.io.Externalizable;

import org.hit.key.domain.DiscreteDomain;

/**
 * Defines the type that defines key space for a table. A key space can be 
 * as simple as enumeration of all the keys possible or it can be the hash
 * range to which the keys are mapped by a hash function.
 * 
 * @author Balraja Subbiah
 */
public interface Keyspace<S extends Comparable<S>, T extends Comparable<T>>
    extends Externalizable
{
    /**
     * Returns the <code>DiscreteDomain</code> that defines the key space.
     */
    public DiscreteDomain<T> getDomain();
    
    /**
     * Maps the key to target key space.
     */
    public T map(S key);
}
