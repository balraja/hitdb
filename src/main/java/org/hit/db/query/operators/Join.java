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
import org.hit.db.model.Row;
import org.hit.db.model.Table;
import org.hit.util.Pair;
import org.hit.util.Range;

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
     * objects stored in multiple tables.
     */
    private static class MultiTableIterator implements Iterator<Row>
    {
        private final List<String> myTables;
        
        private final List<List<Row>> myTableData;
        
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
                        Collections2.transform(
                          table.findMatching(
                               MatchAllPredicate.INSTANCE),
                          new Function<Persistable<?>, Row>() 
                          {
                              public Row apply(Persistable<?> persistable) {
                                  return (Row) persistable;
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
        public Row next()
        {
            Map<String, Row> tableQueryableMap = new HashMap<>();
            for (int tableIndex = 0; tableIndex < myTables.size(); tableIndex++)
            {
                tableQueryableMap.put(myTables.get(tableIndex),
                                      myTableData.get(tableIndex)
                                                 .get(myIndex[tableIndex]));
            }
            
            myHasData = incrIndex();
            return new TableIndexedRow(tableQueryableMap);
        }
        
        private boolean incrIndex()
        {
            int indexToIncrement = -1;
            for (int tableIndex = (myIndex.length - 1); 
                 tableIndex >= 0; 
                 tableIndex--)
            {
                if (myIndex[tableIndex] < (mySizeArray[tableIndex] - 1)) {
                    indexToIncrement = tableIndex;
                    break;
                }
            }
            if (indexToIncrement > -1) {
                myIndex[indexToIncrement]++;
                for (int i = (indexToIncrement + 1); 
                     i < myIndex.length;
                     i++)
                {
                    myIndex[i] = 0;
                }
                return true;
            }
            return false;
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
    
    private Condition myFilter;
    
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
    public Join(Pair<List<String>, Condition> joinCondition,
                Condition                     filter)
    {
        myJoinCondition = joinCondition;
        myFilter        = filter;
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Row> getResult(Database database)
    {
        Iterator<Row> itr = 
            new MultiTableIterator(myJoinCondition.getFirst(),
                                   database);
        List<Row> result = new ArrayList<>();
        while (itr.hasNext()) {
            Row row = itr.next();
            if (   myJoinCondition.getSecond().isValid(row)
                && (myFilter == null || myFilter.isValid(row))) 
            {
                result.add(row);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>> void updateRange(Range<K> newRange)
    {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryOperator cloneOperator()
    {
        return new Join(
            new Pair<List<String>, Condition>(
                new ArrayList<>(myJoinCondition.getFirst()),
                myJoinCondition.getSecond().cloneCondition()), 
            myFilter.cloneCondition());
    }
}
