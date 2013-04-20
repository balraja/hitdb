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

import java.util.Collection;

import org.hit.db.model.Queryable;

import com.google.common.base.Function;

/**
 * Defines the contract for the namespace that can used for capturing
 * aggregate information. This class is final and non-initializable.
 * 
 * @author Balraja Subbiah
 */
public final class Aggregate
{
    /**
     * Defines an enum to capture various aggregating functions.
     * 
     * @author Balraja Subbiah
     */
    public static enum ID
    {
        MIN,
        MAX,
        AVG,
        CNT,
        SUM
    }
    
    /** Type for capturing the aggregation results */
    public class AggregatedValue
    {
        private final Object myKey;
        
        private final double myAggregate;

        /**
         * CTOR
         */
        public AggregatedValue(Object key, double aggregate)
        {
            super();
            myKey = key;
            myAggregate = aggregate;
        }

        /**
         * Returns the value of key
         */
        public Object getKey()
        {
            return myKey;
        }

        /**
         * Returns the value of aggregate
         */
        public double getAggregate()
        {
            return myAggregate;
        }
    }
    
    /** 
     * Defines the contract for an interface that can be used for 
     * aggregating values in a list.
     */
    public static class Accumulator
    {
        private final ID myID;
        
        private double myResult;
        
        private int myCount = 0;
        
       /**
         * CTOR
         */
        public Accumulator(ID id)
        {
            myID = id;
            myCount = 0;
            switch(myID) {
            case MAX:
                myResult = Double.MIN_VALUE;
                break;
            case MIN:
                myResult = Double.MAX_VALUE;
                break;
            default:
                myResult = Double.valueOf(0.0);
            }
        }

        /** Accumulate the given value to the list of old values */
        public void accumulate(Number value)
        {
            switch (myID) {
            case AVG:
            case SUM:
                myResult += value.doubleValue();
                break;
            case CNT:
                break;
            case MAX:
                myResult = Math.max(myResult, value.doubleValue());
                break;
            case MIN:
                myResult = Math.min(myResult, value.doubleValue());
                break;
            }
            myCount++;
        }
       
        /** Returns the result of accumulation */
        public Number getResult()
        {
           if (myID == ID.AVG) {
               return Double.valueOf(myResult / (double) myCount);
           }
           else {
               return Double.valueOf(myResult);
           }
        }
    }
    
    /**
     * Defines the contract for an function that can be used for 
     * performing aggregation operations.
     */
    public static class Aggregator implements Function<Collection<Queryable>, 
                                                       Number>
    {
        private final Aggregate.ID myID;
        
        private final String myColumnName;

        /**
         * CTOR
         */
        public Aggregator(ID id, String columnName)
        {
            super();
            myID = id;
            myColumnName = columnName;
        }
        
        public Number apply(Collection<Queryable> aggregatingCollection)
        {
            Accumulator accumulator = new Accumulator(myID);
            for (Queryable queryable : aggregatingCollection) {
                Number value = (Number) queryable.getFieldValue(myColumnName);
                accumulator.accumulate(value);
            }
            return accumulator.getResult();
        }
    }
}
