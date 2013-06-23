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

package org.hit.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

/**
 * The object that loads all the properties used by the system from a 
 * file. This file should be specified as a system property like <code>
 * -Dapp.properties.file=FILE_NAME</code>
 * 
 * @author Balraja Subbiah
 */
public final class ApplicationProperties
{
    /**
     * The system property that specifies the name of file that contains
     * all the properties used by the system.
     */
    private static final String APP_FILE_NAME_PROPERTY = "app.properties.file";
    
    private static Properties ourAppProperties = new Properties();
    
    static {
        String file = System.getProperty(APP_FILE_NAME_PROPERTY);
        if (file != null) {
            try {
                InputStream fileStream = 
                    ApplicationProperties.class.getClassLoader()
                                               .getResourceAsStream(file);
                Reader reader = 
                    new InputStreamReader(fileStream);
                ourAppProperties.load(reader);
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    
    /** Returns the value of property corresponding to the given name */
    public static String getProperty(String propertyName)
    {
        String value =  ourAppProperties.getProperty(propertyName);
        if (value == null) {
            value = System.getProperty(propertyName);
        }
        return value;
    }
    
    /** CTOR */
    private ApplicationProperties()
    {
    }
}
