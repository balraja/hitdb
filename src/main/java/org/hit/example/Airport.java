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

import org.hit.db.model.Persistable;

/**
 * Defines a class that encapsulates the information about airports.
 * 
 * @author Balraja Subbiah
 */
public class Airport implements Persistable<Long>
{
    private long myID;
    
    private String myName;
    
    private String myCity;
    
    private String myCountry;
    
    private String myIATACode;
    
    private double myLatitude;
    
    private double myLongitude;
    
    private double myAltitude;
    
    private float myDST;
    
    /**
     * CTOR
     */
    public Airport()
    {
    }
    
    /**
     * CTOR
     */
    public Airport(long iD, 
                   String name, 
                   String city, 
                   String country,
                   String iATACode, 
                   double latitude, 
                   double longitude,
                   double altitude, 
                   float dST)
    {
        super();
        myID = iD;
        myName = name;
        myCity = city;
        myCountry = country;
        myIATACode = iATACode;
        myLatitude = latitude;
        myLongitude = longitude;
        myAltitude = altitude;
        myDST = dST;
    }
    
    /**
     * Returns the value of iD
     */
    public long getID()
    {
        return myID;
    }

    /**
     * Returns the value of name
     */
    public String getName()
    {
        return myName;
    }

    /**
     * Returns the value of city
     */
    public String getCity()
    {
        return myCity;
    }

    /**
     * Returns the value of country
     */
    public String getCountry()
    {
        return myCountry;
    }

    /**
     * Returns the value of iATACode
     */
    public String getIATACode()
    {
        return myIATACode;
    }

    /**
     * Returns the value of latitude
     */
    public double getLatitude()
    {
        return myLatitude;
    }

    /**
     * Returns the value of longitude
     */
    public double getLongitude()
    {
        return myLongitude;
    }

    /**
     * Returns the value of altitude
     */
    public double getAltitude()
    {
        return myAltitude;
    }

    /**
     * Returns the value of dST
     */
    public float getDST()
    {
        return myDST;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFieldValue(String fieldName)
    {
        switch (fieldName) {
        case "name" : return myName;
        case "city" : return myCity;
        case "country" : return myCountry;
        case "iata_code" : return myIATACode;
        case "latitude" : return myLatitude;
        case "longitude" : return myLongitude;
        case "dst" : return myDST;
        default : return this;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Long primaryKey()
    {
        return Long.valueOf(myID);
    }
}
