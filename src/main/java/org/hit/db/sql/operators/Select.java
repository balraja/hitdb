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

package org.hit.db.sql.operators;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Row;

import com.google.common.collect.Lists;

/**
 * Extends <Code>Decorator</code> to support aggregation on top of 
 * filtered rows.
 * 
 * @author Balraja Subbiah
 */
public class Select extends Decorator
{
    private String myTableName;
    
    private Map<String, AggregationID> mySelectColumns;
    
    /**
     * CTOR
     */
    public Select()
    {
        this(null, null, null);
    }

    /**
     * CTOR
     */
    public Select(QueryOperator operator, 
                  String tableName,
                  Map<String, AggregationID> selectColumns)
    {
        super(operator);
        myTableName = tableName;
        mySelectColumns = selectColumns;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<Row>
        doPerformOperation(Collection<Row> toBeOperatedCollection)
    {
        // A special handling for select * and select count(*) cases.
        AggregationID aggregationID = 
            mySelectColumns.get(ColumnNameUtil.ALL_COLUMNS);
        // Aggregation can only be cnt.
        if (aggregationID != null) {
            
            AggregationResult result = 
                new AggregationResult(
                    new SelectAggregateKey(ColumnNameUtil.ALL_COLUMNS_SYMBOLIC),
                    toBeOperatedCollection.size());
            
            result.setAggregate(ColumnNameUtil.ALL_COLUMNS_SYMBOLIC,
                                new GroupValue(aggregationID));
            return Lists.<Row>newArrayList(result);
        }
        else if (mySelectColumns.containsKey(ColumnNameUtil.ALL_COLUMNS)) {
            return toBeOperatedCollection;
        }
        
        boolean hasAggregation = 
            mySelectColumns.values().iterator().next() != null;
        
        if (hasAggregation) {
            AggregationResult result = 
                new AggregationResult(
                    new SelectAggregateKey(myTableName),
                    toBeOperatedCollection.size());
            
            for (Map.Entry<String, AggregationID> selectedColumn : 
                     mySelectColumns.entrySet())
            {
                Aggregator aggregator = 
                    new Aggregator(
                        selectedColumn.getValue(), selectedColumn.getKey());
                
                result.setAggregate(selectedColumn.getKey(),
                                    aggregator.apply(toBeOperatedCollection));

            }
            return Lists.<Row>newArrayList(result);
        }
        else {
            return toBeOperatedCollection;
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
        mySelectColumns = (Map<String, AggregationID>) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryOperator cloneOperator()
    {
        return new Select(getDecoratedOperator().cloneOperator(), 
                          myTableName,
                          new HashMap<String, AggregationID>(mySelectColumns));
    }
}
