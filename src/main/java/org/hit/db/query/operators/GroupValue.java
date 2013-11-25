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

/**
 * Defines a type that can be used for capturing aggregate 
 * value for a group.
 * 
 * @author Balraja Subbiah
 */
public class GroupValue
{
    private AggregationID myID;
    
    private double myResult;
    
    /**
     * CTOR
     */
    public GroupValue(AggregationID id)
    {
        myID = id;
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

    /**
     * Returns the value of iD
     */
    public AggregationID getID()
    {
        return myID;
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
    }
   
    /** Returns the result of accumulation */
    public double getResult()
    {
        return myResult;
    }
}
