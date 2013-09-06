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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Collections;

import org.hit.db.model.Database;
import org.hit.db.model.Persistable;
import org.hit.db.model.Predicate;
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
public class Select implements QueryOperator
{
    private String myTableName;
    
    private Condition myFilteringCondition;
    
    /**
     * CTOR
     */
    public Select()
    {
        myTableName = null;
        myFilteringCondition = null;
    }

    /**
     * CTOR
     */
    public Select(String tableName, Condition filteringCondition)
    {
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
            Predicate predicate = 
                myFilteringCondition != null ?
                    new PredicateAdapter(myFilteringCondition)
                    : new Predicate() {
                            @Override
                            public boolean isInterested(Queryable queryable)
                            {
                                return true;
                            }
                        };
            return Collections2.transform(
                 table.findMatching(predicate),
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myTableName);
        out.writeBoolean(myFilteringCondition != null);
        if (myFilteringCondition != null) {
            out.writeObject(myFilteringCondition);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myTableName = in.readUTF();
        boolean isFCAvailable = in.readBoolean();
        if (isFCAvailable) {
            myFilteringCondition = (Condition) in.readObject();
        }
        else {
            myFilteringCondition = null;
        }
    }
}
