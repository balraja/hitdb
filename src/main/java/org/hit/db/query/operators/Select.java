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
import java.util.Map;

import javax.management.RuntimeErrorException;

import org.hit.db.model.Queryable;
import org.hit.db.query.operators.Aggregate.Aggregator;
import org.hit.db.query.operators.Aggregate.ID;

import com.google.common.collect.Lists;

/**
 * Extends <Code>Decorator</code> to support aggregation on top of 
 * filtered rows.
 * 
 * @author Balraja Subbiah
 */
public class Select extends Decorator
{
    private Map<String, Aggregate.ID> mySelectColumns;
    
    /**
     * CTOR
     */
    public Select()
    {
        this(null, null);
    }

    /**
     * CTOR
     */
    public Select(QueryOperator operator, Map<String, ID> selectColumns)
    {
        super(operator);
        mySelectColumns = selectColumns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<Queryable>
        doPerformOperation(Collection<Queryable> toBeOperatedCollection)
    {
        if (mySelectColumns.containsKey(ColumnNameUtil.ALL_COLUMNS)) {
            Aggregate.ID aggregatingFunctionID = 
                mySelectColumns.get(ColumnNameUtil.ALL_COLUMNS);
            if (aggregatingFunctionID != null) {
                if (aggregatingFunctionID == Aggregate.ID.CNT) {
                    QueryableMap result = new QueryableMap();
                    result.setFieldValue(ColumnNameUtil.ALL_COLUMNS, 
                                         Integer.valueOf(
                                             toBeOperatedCollection.size()));
                    return Lists.<Queryable>newArrayList(result);
                }
                else {
                    return new ArrayList<Queryable>();
                }
            }
            else {
                return toBeOperatedCollection;
            }
        }
        else {
            boolean hasAggregation = 
                mySelectColumns.values().iterator().next() != null;
            if (hasAggregation) {
                QueryableMap result = new QueryableMap();
                for (Map.Entry<String, ID> selectedColumn : 
                         mySelectColumns.entrySet())
                {
                    Aggregator aggregator = 
                        new Aggregator(
                            selectedColumn.getValue(), 
                            selectedColumn.getKey());
                    
                    result.setFieldValue(selectedColumn.getKey(),
                                         aggregator.apply(toBeOperatedCollection));

                }
                return Lists.<Queryable>newArrayList(result);
            }
            else {
                return toBeOperatedCollection;
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(mySelectColumns);
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
        mySelectColumns = (Map<String, ID>) in.readObject();
    }
}
