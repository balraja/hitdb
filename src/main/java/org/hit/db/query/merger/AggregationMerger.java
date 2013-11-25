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
package org.hit.db.query.merger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Row;
import org.hit.db.query.operators.AggregatedResult;
import org.hit.db.query.operators.GroupKey;

/**
 * Defines a class that aggregates result generated from different nodes.
 * 
 * @author Balraja Subbiah
 */
public class AggregationMerger implements QueryMerger
{
    private final Map<GroupKey, AggregatedResult> myGroupToAggregateMap;
    
    /**
     * CTOR
     */
    public AggregationMerger()
    {
        myGroupToAggregateMap = new HashMap<>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void addPartialResult(Collection<Row> result)
    {
        for (Row row : result) {
            AggregatedResult newResult = (AggregatedResult) row;
            AggregatedResult existing = 
                myGroupToAggregateMap.get(newResult.getGroupKey());
            if (existing == null) {
                myGroupToAggregateMap.put(newResult.getGroupKey(),
                                          newResult);
            }
            else {
                existing.merge(newResult);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Row> getMergedResult()
    {
        return new ArrayList<Row>(myGroupToAggregateMap.values());
    }
    
}
