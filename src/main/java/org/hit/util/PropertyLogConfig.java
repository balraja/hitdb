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

/**
 * The implementation of <code>LogConfig</code> which reads configuration 
 * information from the properties file.
 * 
 * @author Balraja Subbiah
 */
public class PropertyLogConfig implements LogConfig
{
    private static final String CONST_LOG_FILE_NAME_PROPERTY = 
            "org.hit.util.logFile";
    
    private static final String CONST_LOG_CONFIG_FILE_NAME_PROPERTY = 
            "org.hit.util.logConfigFile";
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getLogFile()
    {
        return System.getProperty(CONST_LOG_FILE_NAME_PROPERTY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLogPropertiesFile()
    {
        return System.getProperty(CONST_LOG_CONFIG_FILE_NAME_PROPERTY);
    }

}
