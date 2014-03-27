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

import org.hit.pool.PooledObjects;

/**
 * A simple class that's responsible for loading <code>Airport</code> from the 
 * 
 * @author Balraja Subbiah
 */
public final class AirportDataLoader extends DataLoader<Airport>
{
    private static final String AIRPORT_FILE = "airports.txt";

    /**
     * CTOR
     */
    public AirportDataLoader()
    {
        super(AIRPORT_FILE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Airport parseFromTokens(String[] parts)
    {
        return PooledObjects.getInstance(Airport.class).initialize(
               Long.parseLong(parts[0]),
               parts[1],
               parts[2],
               parts[3],
               parts[4],
               Double.parseDouble(parts[6]),
               Double.parseDouble(parts[7]),
               Double.parseDouble(parts[8]),
               Float.parseFloat(parts[9]));
    }
}
