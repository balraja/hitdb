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

package org.hit.db.query.operators;

import java.util.Collection;
import java.util.Collections;

import org.hit.db.model.Database;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
import org.hit.db.model.Table;

/**
 * Defines the simple select operation that performs a table scan 
 * and filters results based on the predicate.
 * 
 * @author Balraja Subbiah
 */
public class Select<K extends Comparable<K>, P extends Persistable<K>>
    implements QueryOperator<P>
{
    private final String myTableName;
    
    private final Predicate<K,P> myFilteringPredicate;

    /**
     * CTOR
     */
    public Select(String tableName, Predicate<K,P> filteringPredicate)
    {
        super();
        myTableName = tableName;
        myFilteringPredicate = filteringPredicate;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<P> getResult(Database database)
    {
        Table<K, P> table = database.lookUpTable(myTableName);
        if (table != null) {
            return table.findMatching(myFilteringPredicate);
        }
        else {
            return Collections.emptyList();
        }
    }
}
