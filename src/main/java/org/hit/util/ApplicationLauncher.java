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

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Defines the contract for an ApplicationLauncher that can be used for
 * launching applications.
 * 
 * @author Balraja Subbiah
 */
public class ApplicationLauncher
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(ApplicationLauncher.class);
    
    private final Application myApplication;
    
    /**
     * CTOR
     */
    public ApplicationLauncher(Application application)
    {
        myApplication = application;
    }
    
    /**
     * Launches the application.
     */
    public void launch()
    {
        Thread.setDefaultUncaughtExceptionHandler(
            new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e)
                {
                    LOG.severe("Uncaught exception in thread " + t.getName());
                    LOG.log(Level.SEVERE, e.getMessage(), e);
                    myApplication.stop();
                }
            });;
            
        myApplication.start();
        
        Runtime.getRuntime().addShutdownHook(
            new Thread()
            {
                @Override
                public void run()
                {
                    myApplication.stop();
                }
            });
        
        while(true) {
            // Add an infinite loop so that main application doesn't stop.
        }
    }
}
