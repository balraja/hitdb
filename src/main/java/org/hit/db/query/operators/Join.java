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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.hit.db.model.Database;
import org.hit.db.model.Persistable;
import org.hit.db.model.Queryable;
import org.hit.db.model.Table;
import org.hit.util.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Defines the contract for <code>QueryOperator</code> that can be used 
 * for supporting the joins.
 * 
 * @author Balraja Subbiah
 */
public class Join implements QueryOperator
{
    /**
     * Defines an iterator that generates the cross product of 
     * objects stored in multiple columns.
     */
    private static class MultiTableIterator implements Iterator<Queryable>
    {
        private final List<String> myTables;
        
        private final List<List<Queryable>> myTableData;
        
        private final int[] mySizeArray;
        
        private int[] myIndex;
        
        private boolean myHasData;

        /**
         * CTOR
         */
        public MultiTableIterator(List<String> tables, Database database)
        {
            myTables = tables;
            myTableData = new ArrayList<>();
            for (String tableName : myTables) {
                Table<? extends Comparable<?>,? extends Persistable<?>> table = 
                      database.lookUpTable(tableName);
                myTableData.add(
                    new ArrayList<>(
                          Collections2.transform(table.findMatching(
                               MatchAllPredicate.INSTANCE),
                          new Function<Persistable<?>, Queryable>() 
                          {
                              public Queryable apply(Persistable<?> persistable) {
                                  return (Queryable) persistable;
                              }
                          })));
            }
            myIndex = new int[myTables.size()];
            Arrays.fill(myIndex, 0);
            
            mySizeArray = new int[myTables.size()];
            for (int i = 0; i < myTables.size(); i++) {
                mySizeArray[i] = myTableData.get(i).size();
            }
            myHasData = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            return myHasData;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Queryable next()
        {
            Map<String, Queryable> tableQueryableMap = new HashMap<>();
            for (int tableIndex = 0; tableIndex < myTables.size(); tableIndex++)
            {
                tableQueryableMap.put(myTables.get(tableIndex),
                                      myTableData.get(tableIndex)
                                                 .get(myIndex[tableIndex]));
            }
            
            myHasData = false;
            for (int i = 0; i < myIndex.length; i++) {
                if (myIndex[i] < (mySizeArray[i])) {
                    myHasData = true;
                    break;
                }
            }
            
            if (myHasData) {
                incrIndex();
            }
            
            return new TableIndexedQueryable(tableQueryableMap);
        }
        
        private void incrIndex()
        {
            for (int tableIndex = myIndex.length -1; 
                 tableIndex >= 0; 
                 tableIndex--)
            {
                if (myIndex[tableIndex] == (mySizeArray[tableIndex] - 1)) {
                    myIndex[tableIndex] = 0;
                }
                else {
                    myIndex[tableIndex]++;
                    break;
                }
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void remove()
        {
        }
    }
    
    private Pair<List<String>, Condition> myJoinCondition;
    
    /**
     * CTOR
     */
    public Join()
    {
        myJoinCondition = null;
    }
    
    /**
     * CTOR
     */
    public Join(Pair<List<String>, Condition> joinCondition)
    {
        myJoinCondition = joinCondition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Queryable> getResult(Database database)
    {
        Iterator<Queryable> itr = 
            new MultiTableIterator(myJoinCondition.getFirst(),
                                   database);
        
        List<Queryable> result = new ArrayList<>();
        while (itr.hasNext()) {
            Queryable queryable = itr.next();
            if (myJoinCondition.getSecond().isValid(queryable)) {
                result.add(queryable);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myJoinCondition);        
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myJoinCondition = (Pair<List<String>, Condition>) in.readObject();
    }
}
