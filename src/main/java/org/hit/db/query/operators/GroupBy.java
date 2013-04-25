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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.hit.db.model.Queryable;
import org.hit.db.query.operators.Aggregate.Aggregator;
import org.hit.db.query.operators.Aggregate.ID;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Defines the contract for an operator that supports grouping operations
 * 
 * @author Balraja Subbiah
 */
public class GroupBy extends Decorator
{
    private Map<String, Aggregate.ID> myAggregatingColumns;
    
    private SortedSet<String> myGroupingColumns;
    
    /**
     * CTOR
     */
    public GroupBy()
    {
        myAggregatingColumns = null;
        myGroupingColumns = null;
    }
    
    /**
     * CTOR
     */
    public GroupBy(QueryOperator              operator,
                   List<String>                groupingColumns,
                   Map<String, Aggregate.ID>  columns)
    {
        super(operator);
        myGroupingColumns = new TreeSet<>(groupingColumns);
        myAggregatingColumns = columns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<Queryable>
        doPerformOperation(Collection<Queryable> toBeOperatedCollection)
    {
        ListMultimap<GroupKey, Queryable> multimap = ArrayListMultimap.create();
        for (Queryable queryable : toBeOperatedCollection) {
            multimap.put(new GroupKey(myGroupingColumns, queryable),
                         queryable);
        }
        
        List<Queryable> resultCollection = new ArrayList<>();
        for (Map.Entry<GroupKey, Collection<Queryable>> entry :
               multimap.asMap().entrySet())
        {
            QueryableMap result = new QueryableMap();
            for (Map.Entry<String, Aggregate.ID> aggrEntry : 
                    myAggregatingColumns.entrySet())
            {
                if (aggrEntry.getValue() == null) {
                    result.setFieldValue(aggrEntry.getKey(), 
                                         entry.getKey().getValue(
                                             aggrEntry.getKey()));
                }
                else {
                    Aggregator aggregator = 
                        new Aggregator(
                            aggrEntry.getValue(), aggrEntry.getKey());
                    result.setFieldValue(aggrEntry.getKey(),
                                         aggregator.apply(entry.getValue()));
                }
            }
        }
        return resultCollection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myGroupingColumns);
        out.writeObject(myAggregatingColumns);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
        super.readExternal(in);
        myGroupingColumns = (SortedSet<String>) in.readObject();
        myAggregatingColumns = (Map<String, ID>) in.readObject();
    }
}
