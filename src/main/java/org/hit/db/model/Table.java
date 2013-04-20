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

package org.hit.db.model;

import java.util.Collection;

/**
 * Defines the contract for a table in the database.
 * 
 * @author Balraja Subbiah
 */
public interface Table<K extends Comparable<K>, P extends Persistable<K>>
{
    /**
     * Returns the collection of objects from this table that
     * matches the predicate.
     */
    public Collection<P> findMatching(Predicate predicate);
    
    /**
     * Returns the collection of objects from this table that matches the
     * predicate within the specified range.
     */
    public Collection<P> findMatching(Predicate  predicate, 
                                      K          start, 
                                      K          end);
    
    /** Returns row corresponding to a primary key */
    public P getRow(K primarykey);

    /** Returns schema of the table */
    public Schema getSchema();
    
    /**
     * Updates the old object with the new object
     */
    public boolean update(P old, P updated);
}
