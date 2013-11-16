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

import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Row;

/**
 * Defines the contract for a {@link Row} sent in response to an 
 * aggregation query.
 * 
 * @author Balraja Subbiah
 */
public class AggregatedResult implements Row
{
    private GroupKey myGroupKey;
    
    private Map<String, GroupValue> myColumnToAggregateMap;
    
    private long myGroupCount;

    /**
     * CTOR
     */
    public AggregatedResult()
    {
        super();
    }

    /**
     * CTOR
     */
    public AggregatedResult(GroupKey groupKey, int groupCount)
    {
        super();
        myGroupKey = groupKey;
        myGroupCount = groupCount;
        myColumnToAggregateMap = new HashMap<>();
    }
    
    /**
     * Returns the value of groupKey
     */
    public GroupKey getGroupKey()
    {
        return myGroupKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFieldValue(String fieldName)
    {
        GroupValue aggregate = myColumnToAggregateMap.get(fieldName);
        if (aggregate != null) {
            return (aggregate.getID() == AggregationID.AVG ?
                aggregate.getResult() / myGroupCount
                : aggregate.getResult());
        }
        else {
            return myGroupKey.getValue(fieldName);
        }
    }
    
    /**
     * Sets the aggregate value for a column in the given group.
     */
    public void setAggregate(String columnName, GroupValue aggregate)
    {
        myColumnToAggregateMap.put(columnName, aggregate);
    }
    
    public void merge(AggregatedResult newRow)
    {
        myGroupCount += newRow.myGroupCount;
        for (Map.Entry<String, GroupValue> entry : 
                myColumnToAggregateMap.entrySet())
        {
            entry.getValue().accumulate(
                newRow.myColumnToAggregateMap
                        .get(entry.getKey())
                        .getResult());
        }
    }

}
