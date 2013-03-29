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

package org.hit.example;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigInteger;
import java.util.Calendar;

import org.hit.partitioner.KeySpace;

/**
 * Defines the contract for the key space of <code>ClimateDataKey</code>
 * 
 * @author Balraja Subbiah
 */
public class ClimateDataKeySpace implements KeySpace<ClimateDataKey>
{
    private static final int MIN_STATION_NUMBER = 0;
    
    private static final int MAX_STATION_NUMBER = 99 ;
    
    private static final int MIN_YEAR = 1970;
    
    private static final int MAX_YEAR = Calendar.getInstance()
                                                .get(Calendar.YEAR);
    
    private static final int MIN_DAY_OF_YEAR = 1;
    
    private static final int MAX_DAY_OF_YEAR = 365;
    
    private ClimateDataKey myMinKey;
    
    private ClimateDataKey myMaxKey;
    
    /**
     * CTOR
     */
    public ClimateDataKeySpace()
    {
        myMinKey = 
            new ClimateDataKey(MIN_YEAR, MIN_DAY_OF_YEAR, MIN_STATION_NUMBER);
        myMaxKey = 
            new ClimateDataKey(MAX_YEAR, MAX_DAY_OF_YEAR, MAX_STATION_NUMBER);
    }
    

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myMinKey);
        out.writeObject(myMaxKey);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myMinKey = (ClimateDataKey) in.readObject();
        myMaxKey = (ClimateDataKey) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClimateDataKey getMaximum()
    {
        return myMaxKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClimateDataKey getMinimum()
    {
        return myMinKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger getTotalElements()
    {
        return BigInteger.valueOf(
               ((myMaxKey.getStationNumber() - myMinKey.getStationNumber()) + 1)
             * ((myMaxKey.getYear() - myMinKey.getYear()) + 1)
             * ((myMaxKey.getDayOfYear() - myMinKey.getDayOfYear()) + 1));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMinimum(Object min)
    {
        myMinKey = (ClimateDataKey) min;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaximum(Object max)
    {
        myMaxKey = (ClimateDataKey) max;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClimateDataKey
        nextAtOffset(ClimateDataKey currentValue, BigInteger offset)
    {
        long stationRange = 
             ((myMaxKey.getYear() - myMinKey.getYear()) + 1)
           * ((myMaxKey.getDayOfYear() - myMinKey.getDayOfYear()) + 1);
        
        long yearRange = 
            ((myMaxKey.getDayOfYear() - myMinKey.getDayOfYear()) + 1);
        
        long currentOffset = 
            currentValue.getStationNumber() * stationRange
            + currentValue.getYear() * yearRange
            + currentValue.getDayOfYear()
            + offset.longValue();
                             
                     
        long numStations = currentOffset / stationRange;
        
        currentOffset = currentOffset % stationRange;
        
        long numYears = currentOffset / yearRange;
        
        currentOffset = currentOffset % yearRange;
        
        return new ClimateDataKey((int) numYears , 
                                  (int) currentOffset, 
                                  (int) numStations);
    }
}
