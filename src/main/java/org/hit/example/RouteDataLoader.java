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

/**
 * Extends <code>DataLoader</code> to support loading data for <code>Route
 * </code>s from the file.
 * 
 * @author Balraja Subbiah
 */
public class RouteDataLoader extends DataLoader<Route>
{
    private static final String ROUTES_FILE = "routes.txt";
    
    private long myRouteID;

    /**
     * CTOR
     */
    public RouteDataLoader()
    {
        super(ROUTES_FILE);
        myRouteID = 0L;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Route parseFromTokens(String[] parts)
    {
        long srcAirport = Long.parseLong(parts[3]);
        
        if (srcAirport == 3093 && myRouteID < 4) {

            return new Route(
              myRouteID++, 
              Long.parseLong(parts[1]),
              Long.parseLong(parts[3]),
              Long.parseLong(parts[5]),
              parts[6].trim().isEmpty(),
              Integer.parseInt(parts[7]));
        }
        return null;
    }
}
