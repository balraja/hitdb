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

package org.hit.concurrent;

import java.util.Iterator;
import java.util.List;

/**
 * Defines the contract for hash table that maps between keys and values.
 * 
 * @author Balraja Subbiah
 */
public interface HashTable<K,V>
{
    public static final int THRESHOLD = 10;
    
    /** Adds a given key value combination to the table */
    public boolean add(K key, V value);
    
    /** Returns values corresponding to the given key */
    public List<V> get(K key);
    
    /**
     * Returns an iterator over all the values in system.
     */
    public Iterator<V> getAllValues();
    
    /**
     * Removes the given key value pair from the map.
     */
    public boolean remove(K key, V value);
}
