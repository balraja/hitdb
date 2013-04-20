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

package org.hit.db.transactions;

import java.util.Collection;

import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.Schema;

/**
 * Defines the contract for a database table that aids in supporting
 * transactions.
 * 
 * @author Balraja Subbiah
 * @formatter:off
 */
public interface TransactableTable<K extends Comparable<K>, P extends Persistable<K>>
{
    /** Adds the given <code>Transactable</code> to the table */
    public void addToTable(Transactable<K,P> transactable);

    /**
     * Returns the collection of objects from this table that matches the
     * predicate within the specified range.
     */
    public Collection<Transactable<K,P>> findMatching(
       Predicate predicate,
       K         start,
       K         end,
       long      time,
       long      transactionID);

    /**
     * Returns the collection of objects from this table that matches the
     * predicate.
     */
    public Collection<Transactable<K,P>> findMatching(
        Predicate predicate,
        long time,
        long transactionID);

    /** Returns row corresponding to a primary key */
    public Transactable<K,P> getRow(K key, long time, long transactionID);

    /** Returns schema of the table */
    public Schema getSchema();
    
    /**
     * Removes the transactable from the table.
     */
    public void remove(Transactable<K,P> transactable);
}
