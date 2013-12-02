/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease. 

    Copyright (C) 2012  Balraja Subbiah

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

import java.io.File;

/**
 * The implementation of <code>LogConfig</code> which reads configuration 
 * information from the properties file.
 * 
 * @author Balraja Subbiah
 */
public class PropertyLogConfig implements LogConfig
{
    private static final String LOG_FILE_NAME = "hit.log";
    
    public static final String LOG_FILE_NAME_PROPERTY = 
            "org.hit.util.logFile";
    
    public static final String LOG_CONFIG_FILE_NAME_PROPERTY = 
            "org.hit.util.logConfigFile";
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getLogFile()
    {
        String file = ApplicationProperties.getProperty(LOG_FILE_NAME_PROPERTY);
        
        if (file == null) {
            file = System.getProperty("java.io.tmpdir");
            file += (File.separator + LOG_FILE_NAME);
        }
        return file;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLogPropertiesFile()
    {
        return ApplicationProperties.getProperty(LOG_CONFIG_FILE_NAME_PROPERTY);
    }

}
