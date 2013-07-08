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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.util.LogFactory;

/**
 * A simple class that's responsible for loading <code>Airport</code> from the 
 * 
 * @author Balraja Subbiah
 */
public final class AirportDataLoader
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(AirportDataLoader.class);

    private static final String AIRPORT_FILE = "airports.txt";

    private static final String COMMENT = "/**";

    /**
     * A helper method to read the data from test file
     */
    public static List<Airport> loadTestData()
    {
        String line = null;
        List<Airport> airportData = new ArrayList<>();
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(
                     HitDbTest.class.getClassLoader()
                                    .getResourceAsStream(AIRPORT_FILE))))
        {
            while ((line = reader.readLine()) != null) {

                if (line.startsWith(COMMENT)) {
                    continue;
                }

                String[] parts = line.split(",");
                airportData.add(new Airport(
                    Long.parseLong(parts[0]),
                    parts[1],
                    parts[2],
                    parts[3],
                    parts[4],
                    Double.parseDouble(parts[6]),
                    Double.parseDouble(parts[7]),
                    Double.parseDouble(parts[8]),
                    Float.parseFloat(parts[9])));

            }
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return airportData;
    }

}
