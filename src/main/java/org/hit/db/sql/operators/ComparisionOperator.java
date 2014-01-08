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

/** 
 * An enum to define various numeric comparison operators that can be used.
 */
public enum ComparisionOperator
{
    LT("<") {

        @Override
        public boolean compare(double value1, double value2)
        {
            return value1 < value2;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public boolean compare(Object value1, Object value2)
        {
            return ((Comparable) value1).compareTo(value2) < 0;
        }
    }
    ,
    GT(">") {
        @Override
        public boolean compare(double value1, double value2)
        {
            return value1 > value2;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public boolean compare(Object value1, Object value2)
        {
            return ((Comparable) value1).compareTo(value2) > 0;
        }
    },
    EQ("=") {
        @Override
        public boolean compare(double value1, double value2)
        {
            return value1 == value2;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public boolean compare(Object value1, Object value2)
        {
            return ((Comparable) value1).compareTo(value2) == 0;
        }
    },
    NE("!=") {
        @Override
        public boolean compare(double value1, double value2)
        {
            return value1 != value2;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public boolean compare(Object value1, Object value2)
        {
            return ((Comparable) value1).compareTo(value2) != 0;
        }
    },
    LE("<=") {
        @Override
        public boolean compare(double value1, double value2)
        {
            return value1 <= value2;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public boolean compare(Object value1, Object value2)
        {
            return ((Comparable) value1).compareTo(value2) <= 0;
        }
    },
    GE(">=") {
        @Override
        public boolean compare(double value1, double value2)
        {
            return value1 >= value2;
        }
        
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public boolean compare(Object value1, Object value2)
        {
            return ((Comparable) value1).compareTo(value2) >= 0;
        }
    };
    
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
    
    public abstract boolean compare(double value1, double value2);
    
    public abstract boolean compare(Object value1, Object value2);

    static ComparisionOperator getOperatorForSymbol(String symbol)
    {
        for (ComparisionOperator operator : values()) {
            if (symbol.contains(operator.getSymbol())) {
                return operator;
            }
        }
        return null;
    }
    
    static boolean isEqualityComparator(ComparisionOperator operator)
    {
        return operator == EQ || operator == NE;
    }
    
    static boolean isLTComparator(ComparisionOperator operator)
    {
        return operator == LT || operator == LE;
    }
    
    static boolean isGTComparator(ComparisionOperator operator)
    {
        return operator == GT || operator == GE;
    }
}