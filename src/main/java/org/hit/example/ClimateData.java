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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.db.model.Column;
import org.hit.db.model.Persistable;

/**
 * A simple table that captures the weather database.
 *
 * @author Balraja Subbiah
 */
public class ClimateData implements Persistable<ClimateDataKey>, Externalizable
{
    private ClimateDataKey myKey;

    private int myMaxTemperature;

    private int myMeanDewPoint;

    private int myMeanTemperature;

    private int myMeanWindSpeed;

    private int myMinTemperature;

    /**
     * CTOR
     */
    public ClimateData()
    {
        this(null, -1, -1, -1, -1, -1);
    }

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
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ClimateData other = (ClimateData) obj;
        if (myKey == null) {
            if (other.myKey != null) {
                return false;
            }
        }
        else if (!myKey.equals(other.myKey)) {
            return false;
        }
        if (myMaxTemperature != other.myMaxTemperature) {
            return false;
        }
        if (myMeanDewPoint != other.myMeanDewPoint) {
            return false;
        }
        if (myMeanTemperature != other.myMeanTemperature) {
            return false;
        }
        if (myMeanWindSpeed != other.myMeanWindSpeed) {
            return false;
        }
        if (myMinTemperature != other.myMinTemperature) {
            return false;
        }
        return true;
    }

    /**
     * Returns the value of key
     */
    public ClimateDataKey getKey()
    {
        return myKey;
    }

    /**
     * Returns the value of maxTemperature
     */
    public int getMaxTemperature()
    {
        return myMaxTemperature;
    }

    /**
     * Returns the value of meanDewPoint
     */
    public int getMeanDewPoint()
    {
        return myMeanDewPoint;
    }

    /**
     * Returns the value of meanTemperature
     */
    public int getMeanTemperature()
    {
        return myMeanTemperature;
    }

    /**
     * Returns the value of meanWindSpeed
     */
    public int getMeanWindSpeed()
    {
        return myMeanWindSpeed;
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
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((myKey == null) ? 0 : myKey.hashCode());
        result = prime * result + myMaxTemperature;
        result = prime * result + myMeanDewPoint;
        result = prime * result + myMeanTemperature;
        result = prime * result + myMeanWindSpeed;
        result = prime * result + myMinTemperature;
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClimateDataKey primaryKey()
    {
        return myKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myKey = (ClimateDataKey) in.readObject();
        myMeanTemperature = in.readInt();
        myMeanDewPoint = in.readInt();
        myMeanWindSpeed = in.readInt();
        myMaxTemperature = in.readInt();
        myMinTemperature = in.readInt();

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myKey);
        out.writeInt(myMeanTemperature);
        out.writeInt(myMeanDewPoint);
        out.writeInt(myMeanWindSpeed);
        out.writeInt(myMaxTemperature);
        out.writeInt(myMinTemperature);
    }
}
