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

package org.hit.db.sql.parser;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hit.db.sql.operators.AggregationID;
import org.hit.db.sql.operators.Condition;
import org.hit.util.Pair;

/**
 * Defines the contract for the query builder class which can be used for 
 * building an executable <code>Query</code> from the abstract syntax tree
 * of the parsed sql statement.
 * 
 * @author Balraja Subbiah
 */
public class QueryAttributes
{
    private Map<String, AggregationID> mySelectedColumns;
    
    private List<String> myGroupByAttributes;
    
    private Condition myHavingCondition;
    
    private String myTableName;
    
    private Pair<List<String>, Condition> myJoinCriteria;
    
    private List<String> myTableCrossProduct;
    
    private Condition myWhereCondition;
    
    private Map<String, Boolean> myOrderbyCriterion;
    
    private int myLimit = Integer.MAX_VALUE;
    
    /**
     * CTOR
     */
    public QueryAttributes()
    {
        mySelectedColumns = new HashMap<>();
    }

    /**
     * Returns the value of selectedColumns
     */
    public Map<String, AggregationID> getSelectedColumns()
    {
        return mySelectedColumns;
    }

    /**
     * Setter for the selectedColumns
     */
    public void setSelectedColumns(Map<String, AggregationID> selectedColumns)
    {
        mySelectedColumns = selectedColumns;
    }
    
    /**
     * Returns the value of groupByAttributes
     */
    public List<String> getGroupByAttributes()
    {
        return myGroupByAttributes;
    }

    /**
     * Setter for the groupByAttributes
     */
    public void setGroupByAttributes(List<String> groupByAttributes)
    {
        myGroupByAttributes = groupByAttributes;
    }
    
    /**
     * Returns the value of havingCondition
     */
    public Condition getHavingCondition()
    {
        return myHavingCondition;
    }

    /**
     * Setter for the havingCondition
     */
    public void setHavingCondition(Condition havingCondition)
    {
        myHavingCondition = havingCondition;
    }

    /**
     * Returns the value of tableName
     */
    public String getTableName()
    {
        return myTableName;
    }

    /**
     * Setter for the tableName
     */
    public void setTableName(String tableName)
    {
        myTableName = tableName;
    }

    /**
     * Returns the value of joinCriteria
     */
    public Pair<List<String>, Condition> getJoinCriteria()
    {
        return myJoinCriteria;
    }

    /**
     * Setter for the joinCriteria
     */
    public void setJoinCriteria(Pair<List<String>, Condition> joinCriteria)
    {
        myJoinCriteria = joinCriteria;
    }

    /**
     * Returns the value of tableCrossProduct
     */
    public List<String> getTableCrossProduct()
    {
        return myTableCrossProduct;
    }

    /**
     * Setter for the tableCrossProduct
     */
    public void setTableCrossProduct(List<String> tableCrossProduct)
    {
        myTableCrossProduct = tableCrossProduct;
    }

    /**
     * Returns the value of whereCondition
     */
    public Condition getWhereCondition()
    {
        return myWhereCondition;
    }

    /**
     * Setter for the whereCondition
     */
    public void setWhereCondition(Condition whereCondition)
    {
        myWhereCondition = whereCondition;
    }

    /**
     * Returns the value of orderbyCriterion
     */
    public Map<String, Boolean> getOrderByCriterion()
    {
        return myOrderbyCriterion;
    }

    /**
     * Setter for the orderbyCriterion
     */
    public void setOrderByCriterion(Map<String, Boolean> orderbyCriterion)
    {
        myOrderbyCriterion = orderbyCriterion;
    }

    /**
     * Returns the value of limit
     */
    public int getLimit()
    {
        return myLimit;
    }

    /**
     * Setter for the limit
     */
    public void setLimit(int limit)
    {
        myLimit = limit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "QueryAttributes [mySelectedColumns=" + mySelectedColumns
               + ", myGroupByAttributes="
               + myGroupByAttributes
               + ", myHavingCondition="
               + myHavingCondition
               + ", myTableName="
               + myTableName
               + ", myJoinCriteria="
               + myJoinCriteria
               + ", myTableCrossProduct="
               + myTableCrossProduct
               + ", myWhereCondition="
               + myWhereCondition
               + ", myOrderbyCriterion="
               + myOrderbyCriterion
               + ", myLimit="
               + myLimit
               + "]";
    }
}
