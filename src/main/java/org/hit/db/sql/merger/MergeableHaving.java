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
package org.hit.db.sql.merger;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.hit.db.model.Row;
import org.hit.db.sql.operators.Condition;

/**
 * Extends {@link QueryResultMerger} to support filtering the merged results
 * based on  the having condition paramters.
 * 
 * @author Balraja Subbiah
 */
public class MergeableHaving implements QueryResultMerger
{
    private final Condition myCondition;
    
    private final AggregationMerger myAggregator;

    /**
     * CTOR
     */
    public MergeableHaving(Condition condition)
    {
        super();
        myCondition = condition;
        myAggregator = new AggregationMerger();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addPartialResult(Collection<Row> result)
    {
        myAggregator.addPartialResult(result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Row> getMergedResult()
    {
        List<Row> result = new LinkedList<>();
        for (Row row : myAggregator.getMergedResult()) {
            if (myCondition.isValid(row)) {
                result.add(row);
            }
        }
        return result;
    }
}
