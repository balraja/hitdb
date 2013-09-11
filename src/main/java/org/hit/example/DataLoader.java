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
 * Defines the contract for an interface for loading data from files.
 * 
 * @author Balraja Subbiah
 */
public abstract class DataLoader<T>
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(DataLoader.class);

    private static final String COMMENT = "/**";
    
    private final String myFilePath;
    
    /**
     * CTOR
     */
    public DataLoader(String filePath)
    {
        myFilePath = filePath;
    }

    /**
     * A helper method to read the data from test file
     */
    public List<T> loadTestData()
    {
        String line = null;
        List<T> result = new ArrayList<>();
        try (BufferedReader reader =
                 new BufferedReader(new InputStreamReader(
                     HitDbTest.class.getClassLoader()
                                    .getResourceAsStream(myFilePath))))
        {
            while ((line = reader.readLine()) != null) {

                if (line.startsWith(COMMENT)) {
                    continue;
                }

                String[] parts = line.split(",");
                result.add(parseFromTokens(parts));

            }
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return result;
    }
    
    protected abstract T parseFromTokens(String[] parts);
}
