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
import org.hit.db.model.Queryable;
import org.hit.db.model.Table;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Defines the simple select operation that performs a table scan 
 * and filters results based on the predicate.
 * 
 * @author Balraja Subbiah
 */
public class Select implements QueryOperator<Collection<Queryable>>
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
    public Collection<Queryable> getResult(Database database)
    {
        Table<? extends Comparable<?>,
              ? extends Persistable<?>> table = 
                  database.lookUpTable(myTableName);
        
        if (table != null) {
            return Collections2.transform(table.findMatching(
                new PredicateAdapter(myFilteringCondition)),
                new Function<Persistable<?>, Queryable>() 
                {
                    public Queryable apply(Persistable<?> persistable) {
                        return (Queryable) persistable;
                    }
                });
        }
        else {
            return Collections.emptyList();
        }
    }
}
