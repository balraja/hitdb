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
 * Extends <code>DataLoader</code> to support loading <code>Airline</code>
 * data.
 * 
 * @author Balraja Subbiah
 */
public class AirlineDataLoader extends DataLoader<Airline>
{
    private static final String AIRLINE_FILE = "airlines.txt";
    
    /**
     * CTOR
     */
    public AirlineDataLoader()
    {
        super(AIRLINE_FILE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Airline parseFromTokens(String[] parts)
    {
        return PooledObjects.getInstance(Airline.class)
                            .initialize(Long.valueOf(parts[0]),
                                        parts[1]);
    }
}
