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

/**
 * Defines the key for the climate data represnted by <code>ClimateData</code>
 *
 * @author Balraja Subbiah
 */
public class ClimateDataKey
    implements Comparable<ClimateDataKey>, Externalizable
{
    private int myDayOfYear;

    private int myStationNumber;

    private int myYear;


    /**
     * CTOR
     */
    public ClimateDataKey()
    {
        this(-1, -1, -1);
    }

    /**
     * CTOR
     */
    public ClimateDataKey(int year, int dayOfYear, int stationNumber)
    {
        super();
        myYear = year;
        myDayOfYear = dayOfYear;
        myStationNumber = stationNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(ClimateDataKey other)
    {
        if (myStationNumber == other.getStationNumber()) {
            if (myYear == other.getYear()) {
                if (myDayOfYear == other.getDayOfYear()) {
                    return 0;
                }
                else {
                    return myDayOfYear - other.getDayOfYear();
                }
            }
            else {
                return myYear - other.getYear();
            }
        }
        else {
            return myStationNumber - other.getStationNumber();
        }
    }

    /**
     * Returns the value of dayOfYear
     */
    public int getDayOfYear()
    {
        return myDayOfYear;
    }

    /**
     * Returns the value of stationNumber
     */
    public int getStationNumber()
    {
        return myStationNumber;
    }

    /**
     * Returns the value of year
     */
    public int getYear()
    {
        return myYear;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myStationNumber = in.readInt();
        myYear = in.readInt();
        myDayOfYear = in.readInt();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(myStationNumber);
        out.writeInt(myYear);
        out.writeInt(myDayOfYear);
    }
}
