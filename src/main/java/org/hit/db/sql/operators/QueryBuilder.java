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

import java.util.Map;

import org.hit.db.sql.merger.AggregationMerger;
import org.hit.db.sql.merger.MergeableHaving;
import org.hit.db.sql.merger.QueryResultMerger;
import org.hit.db.sql.parser.QueryAttributes;
import org.hit.util.Pair;

/**
 * Defines a query builder that be used for building the <code>Query</code>
 * 
 * @author Balraja Subbiah
 */
public class QueryBuilder
{
    private final QueryAttributes myQueryAttributes;
    
    /**
     * CTOR
     */
    public QueryBuilder(QueryAttributes queryAttributes)
    {
        myQueryAttributes = queryAttributes;
    }
    
    /** 
     * Returns the <code>Query</code> built from the <code>QueryAttributes
     * </code>.
     */
    public Pair<QueryAdaptor, QueryResultMerger> buildQuery(boolean isDistributed) 
        throws QueryBuildingException
    {
        if (   myQueryAttributes.getSelectedColumns() == null 
            || myQueryAttributes.getSelectedColumns().isEmpty())
        {
            throw new QueryBuildingException(
                "Please select some columns for the output");
        }
        
        for (Map.Entry<String, AggregationID> selectedColumn : 
                 myQueryAttributes.getSelectedColumns().entrySet())
        {
            if (selectedColumn.getKey().equals(ColumnNameUtil.ALL_COLUMNS)
                && selectedColumn.getValue() != null
                && selectedColumn.getValue() != AggregationID.CNT) 
            {
                throw new QueryBuildingException(
                    "Only CNT aggregation is valid in select * format");

            }
        }
        
        boolean hasSelectAggregation = false, hasNonAggregatedColumns = false;
        for (Map.Entry<String, AggregationID> selectedColumn : 
            myQueryAttributes.getSelectedColumns().entrySet())
        {
            if (selectedColumn.getValue() != null) {
                hasSelectAggregation = true;
            }
            else {
                hasNonAggregatedColumns = true;
            }
        }
        
        if (hasSelectAggregation && hasNonAggregatedColumns) {
            throw new QueryBuildingException(
                "Select cann't contain aggregated and non aggregated columns");
        }
        
        if (myQueryAttributes.getGroupByAttributes() != null) {
            for (Map.Entry<String, AggregationID> selectedColumn : 
                    myQueryAttributes.getSelectedColumns().entrySet())
            {
                if (   selectedColumn.getValue() == null 
                    && !myQueryAttributes.getGroupByAttributes().contains(
                           selectedColumn.getKey()))
                {
                    throw new QueryBuildingException(
                        " The column " + selectedColumn.getKey() + " is "
                        + "  selected but it's not a grouping column");
                }
            }
        }
        
        QueryOperator operator = null;
        if (   myQueryAttributes.getTableName() != null 
            && (   myQueryAttributes.getWhereCondition() != null
                || (   myQueryAttributes.getWhereCondition() == null 
                    && myQueryAttributes.getJoinCriteria() == null)))
        {
            operator = new Where(myQueryAttributes.getTableName(),
                                  myQueryAttributes.getWhereCondition());
        }
        else if (myQueryAttributes.getJoinCriteria() != null) {
            operator = new Join(myQueryAttributes.getJoinCriteria(),
                                myQueryAttributes.getWhereCondition());
        }
        
        if (myQueryAttributes.getGroupByAttributes() != null) {
            if (operator == null) {
                throw new QueryBuildingException(
                    "Please specify the table or join whose data needs to be "
                   + " aggregated");
            }
            else {
                operator = new GroupBy(operator, 
                                       myQueryAttributes.getGroupByAttributes(),
                                       myQueryAttributes.getSelectedColumns());
            }
            
            if (myQueryAttributes.getHavingCondition() != null
                && !isDistributed) 
            {
                
                operator = new Having(operator, 
                                      myQueryAttributes.getHavingCondition());
            }
        }
        else {
            String tableName = 
                myQueryAttributes.getJoinCriteria() != null ?
                        myQueryAttributes.getJoinCriteria()
                                         .getFirst()
                                         .toString()
                        : myQueryAttributes.getTableName();
                                         
            operator = new Select(operator,
                                  tableName,
                                  myQueryAttributes.getSelectedColumns());
        }
        
        QueryResultMerger queryMerger = 
            isDistributed ?
                myQueryAttributes.getHavingCondition() != null ?
                    new MergeableHaving(myQueryAttributes.getHavingCondition())
                    : new AggregationMerger()
            : null;
                    
        return new Pair<>(new QueryAdaptor(operator), queryMerger);
    }
}
