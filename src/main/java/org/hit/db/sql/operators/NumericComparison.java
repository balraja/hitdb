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

import org.hit.db.model.Row;
import org.hit.util.Range;

/**
 * Defines the contract for a class that can be used for comparing 
 * column values against numeric constants.
 * 
 * @author Balraja Subbiah
 */
public class NumericComparison implements Condition
{
    private String[] myColumnNames;

    private ComparisionOperator myOperator;
    
    private double myComparedValue;
    
    /**
     * CTOR
     */
    public NumericComparison()
    {
        myColumnNames = null;
        myOperator = null;
        myComparedValue = 0.0D;
    }
    
    /**
     * CTOR
     */
    public NumericComparison(String columnName, 
                              String operator,
                              String comparedValue)
    {
        this(columnName, 
             ComparisionOperator.getOperatorForSymbol(operator),
             Double.parseDouble(comparedValue));
    }

    /**
     * CTOR
     */
    public NumericComparison(String columnName, 
                              ComparisionOperator operator,
                              double comparedValue)
    {
        myColumnNames = ColumnNameUtil.nestedColumnNames(columnName);
        myOperator = operator;
        myComparedValue = comparedValue;
    }
    
    /**
     * CTOR
     */
    public NumericComparison(
        String[] columnNames,
        ComparisionOperator operator,
        double comparedValue)
    {
        super();
        myColumnNames = columnNames;
        myOperator = operator;
        myComparedValue = comparedValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "Columns" + myColumnNames + " " + myOperator.name() + " " + myComparedValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(Row record)
    {
        Object fieldValue = ColumnNameUtil.getValue(record, myColumnNames);
        if (fieldValue != null && fieldValue instanceof Number) {
            Number numericValue = (Number) fieldValue;
            return myOperator.compare(numericValue.doubleValue(), 
                                      myComparedValue);
        }
        else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myColumnNames);
        out.writeUTF(myOperator.name());
        out.writeDouble(myComparedValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myColumnNames = (String[]) in.readObject();
        myOperator = ComparisionOperator.valueOf(in.readUTF());
        myComparedValue = in.readDouble();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>> void updateRange(Range<K> newRange)
    {
        if (!ComparisionOperator.isEqualityComparator(myOperator)) {
            K updatedValue = 
                ComparisionOperator.isGTComparator(myOperator) ?
                    newRange.getMinValue()
                    : newRange.getMaxValue();
               
            if (updatedValue instanceof Number) {
                myComparedValue = ((Number) updatedValue).doubleValue();
            }
            else if (updatedValue instanceof Row) {
                Object fieldValue = 
                    ColumnNameUtil.getValue((Row) updatedValue, myColumnNames);
                if (fieldValue != null && fieldValue instanceof Number) {
                    Number numericValue = (Number) fieldValue;
                    myComparedValue = numericValue.doubleValue();
                }
            }
        }
    }
    
    public Condition cloneCondition()
    {
        return new NumericComparison(
            ColumnNameUtil.copyColumnName(myColumnNames), 
            myOperator, 
            myComparedValue);
    }
}
