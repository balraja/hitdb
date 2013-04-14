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

import org.hit.db.model.Queryable;

/**
 * Defines the contract for a class that can be used for comparing 
 * column values against numeric constants.
 * 
 * @author Balraja Subbiah
 */
public class NumericComparision extends AbstractCondition
{
    /** An enum to define various relational operators that can be applied */
    public static enum ComparisionOperator
    {
        LT("<"),
        GT(">"),
        EQ("="),
        NE("!="),
        LE("<="),
        GE(">=");
        
        private final String mySymbol;
        
        ComparisionOperator(String symbol)
        {
            mySymbol = symbol;
        }
        
        /**
         * Returns the value of symbol
         */
        public String getSymbol()
        {
            return mySymbol;
        }

        static ComparisionOperator getOperatorForSymbol(String symbol)
        {
            for (ComparisionOperator operator : values()) {
                if (symbol.contains(operator.getSymbol())) {
                    return operator;
                }
            }
            return null;
        }
    }

    private final ComparisionOperator myOperator;
    
    private final double myComparedValue;
    
    /**
     * CTOR
     */
    public NumericComparision(String columnName, 
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
    public NumericComparision(String columnName, 
                              ComparisionOperator operator,
                              double comparedValue)
    {
        super(columnName);
        myOperator = operator;
        myComparedValue = comparedValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(Queryable record)
    {
        Object fieldValue = getValue(record);
        if (fieldValue != null && fieldValue instanceof Number) {
            Number numericValue = (Number) fieldValue;
            switch (myOperator) {
            case EQ:
                return numericValue.doubleValue() == myComparedValue;
            case GE:
                return numericValue.doubleValue() == myComparedValue;
            case GT:
                return numericValue.doubleValue() > myComparedValue;
            case LE:
                return numericValue.doubleValue() <= myComparedValue;
            case LT:
                return numericValue.doubleValue() < myComparedValue;
            case NE:
                return numericValue.doubleValue() != myComparedValue;
            default:
                return false;
            }
        }
        else {
            return false;
        }
    }
}
