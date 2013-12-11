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

/**
 * An abstract implementation of application launcher that shares the
 * common code and properties used by server and client launchers.
 *
 * @author Balraja Subbiah
 */
public abstract class AbstractLauncher
{
    protected static final String APP_HOME_PROPERTY = "org.hit.app.home";

    protected static final String CLASSPATH_PREFIX = "CLASSPATH_PREFIX";

    protected static final String HELP = "help";

    protected static final String JAVA_OPTS = "JAVA_OPTS";

    protected static final String LOG_FILE= "log_file";
    
    protected static final String DUMP_FILE = "dump_file";
    
    private static final String BAT = ".bat";
    
    private static final String SH = ".sh";

    /** Returns a property that can be added to a command line */
    protected String addProperty(String PropertyName, String value)
    {
        return " -D" + PropertyName + "=" + value + " ";
    }

    protected String getLauncherScriptSuffix()
    {
        return System.getProperty("os.name")
                     .toLowerCase()
                     .indexOf("win") >= 0 ? BAT : SH;
    }
}
