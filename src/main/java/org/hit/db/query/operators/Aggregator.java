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

import com.google.common.base.Function;

import java.util.Collection;

import org.hit.db.model.Row;

/**
 * Defines the contract for an function that can be used for 
 * performing aggregation operations.
 */
public class Aggregator implements Function<Collection<Row>, 
                                            GroupValue>
{
    private final AggregationID myID;
    
    private final String myColumnName;
    
    /**
     * CTOR
     */
    public Aggregator(AggregationID id, String columnName)
    {
        super();
        myID = id;
        myColumnName = columnName;
    }
    
    public GroupValue apply(Collection<Row> aggregatingCollection)
    {
        GroupValue groupValue = new GroupValue(myID);
        for (Row row : aggregatingCollection) {
            Number value = (Number) row.getFieldValue(myColumnName);
            groupValue.accumulate(value);
        }
        return groupValue;
    }
}