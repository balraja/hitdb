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

import org.hit.db.model.Column;
import org.hit.db.model.Persistable;

/**
 * A simple table that captures the weather database.
 * 
 * @author Balraja Subbiah
 */
public class ClimateData implements Persistable<ClimateDataKey>
{
    private final ClimateDataKey myKey;
    
    private final int myMeanTemperature;
    
    private final int myMeanDewPoint;
    
    private final int myMeanWindSpeed;
    
    private final int myMaxTemperature;
    
    private final int myMinTemperature;

    /**
     * CTOR
     */
    public ClimateData(ClimateDataKey key, 
                       int meanTemperature,
                       int meanDewPoint, 
                       int meanWindSpeed, 
                       int maxTemperature,
                       int minTemperature)
    {
        myKey = key;
        myMeanTemperature = meanTemperature;
        myMeanDewPoint = meanDewPoint;
        myMeanWindSpeed = meanWindSpeed;
        myMaxTemperature = maxTemperature;
        myMinTemperature = minTemperature;
    }

    /**
     * Returns the value of key
     */
    public ClimateDataKey getKey()
    {
        return myKey;
    }

    /**
     * Returns the value of meanTemperature
     */
    public int getMeanTemperature()
    {
        return myMeanTemperature;
    }

    /**
     * Returns the value of meanDewPoint
     */
    public int getMeanDewPoint()
    {
        return myMeanDewPoint;
    }

    /**
     * Returns the value of meanWindSpeed
     */
    public int getMeanWindSpeed()
    {
        return myMeanWindSpeed;
    }

    /**
     * Returns the value of maxTemperature
     */
    public int getMaxTemperature()
    {
        return myMaxTemperature;
    }

    /**
     * Returns the value of minTemperature
     */
    public int getMinTemperature()
    {
        return myMinTemperature;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getValue(Column column)
    {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClimateDataKey primaryKey()
    {
        return myKey;
    }
}
