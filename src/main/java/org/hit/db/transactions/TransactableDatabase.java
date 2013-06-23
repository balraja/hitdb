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

import org.hit.db.model.Persistable;
import org.hit.db.model.Schema;
import org.hit.event.DBStatEvent;

/**
 * Wraps <code>Database</code> to keep track of the keys accessed by a
 * <code>Transaction</code> during it's lifetime.
 * 
 * @author Balraja Subbiah
 */
public interface TransactableDatabase
{
    /** Creates a table with the specified schema in the database */
    public void createTable(Schema schema);

    /** Returns the <code>TransactableTable</code> from the database. */
    public <K extends Comparable<K>, P extends Persistable<K>> TransactableTable<K, P>
        lookUpTable(String tableName);
    
    /**
     * Generates the statistics for database.
     */
    public DBStatEvent getStatistics();
}
