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
import org.hit.db.model.Queryable;
import org.hit.db.model.Table;

/**
 * Defines the simple select operation that performs a table scan 
 * and filters results based on the predicate.
 * 
 * @author Balraja Subbiah
 */
public class Select implements QueryOperator
{
    private final String myTableName;
    
    private final Condition myFilteringCondition;

    /**
     * CTOR
     */
    public Select(String tableName, Condition filteringCondition)
    {
        super();
        myTableName = tableName;
        myFilteringCondition = filteringCondition;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public Collection<Queryable> getResult(Database database)
    {
        Table<?,?> table = database.lookUpTable(myTableName);
        if (table != null) {
            return (Collection<Queryable>) table.findMatching(
                new PredicateAdapter(myFilteringCondition));
        }
        else {
            return Collections.emptyList();
        }
    }
}
