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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;

/**
 * Creates <code>Log</code> writers for various class names.
 * 
 * @author Balraja Subbiah
 */
public class LogFactory
{
    private static LogFactory ourCachedInstance;
    
    private final LogConfig myConfig;
    
    private final Handler myHandler;

    /**
     * CTOR
     */
    private LogFactory(LogConfig config)
    {
        myConfig = config;
        try {
            LogManager.getLogManager().readConfiguration(
                new FileInputStream(myConfig.getLogPropertiesFile()));
            myHandler = new FileHandler(myConfig.getLogFile());
        }
        catch (SecurityException | IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    /** Accessor for the singleton instance of LogFactory */
    public static LogFactory getInstance()
    {
        if (ourCachedInstance == null) {
            ourCachedInstance = new LogFactory(new PropertyLogConfig());
        }
        return ourCachedInstance;
    }
    
    /** Returns <cdoe>Logger</code> for this namespace */
    public Logger getLogger(Class<?> namespaceClass)
    {
        Logger logger = 
            LogManager.getLogManager().getLogger(namespaceClass.getName());
        logger.addHandler(myHandler);
        return logger;
    }
}
