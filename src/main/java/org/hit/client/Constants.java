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
package org.hit.client;

/**
 * An util class to keep track of all the constants used in the client 
 * package.
 * 
 * @author Balraja Subbiah
 */
public final class Constants
{
    public static final String TITLE =
        "Hit Database ";
    
    public static final String COPY_RIGHT =
        "Copyright (C) 2014 Balraja Subbiah";
    
    public static final String BANNER = TITLE + COPY_RIGHT;

    public static final String HELP_MSG = "Use 'help' for assistance";
    
    public static final String APP_ICON_FILE = "app-icon.jpg";
    
    public static final String ABOUT = 
        "Hit stands for Humungous agIle Transactional database. "
        + "This is built and maintained by Balraja Subbiah (C) 2014. "
        + "The project is hosted on git https://github.com/balraja/hitdb.";

    /**
     * CTOR
     */
    private Constants()
    {
        
    }
}
