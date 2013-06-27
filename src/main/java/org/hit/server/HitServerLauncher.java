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

package org.hit.server;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.hit.node.NodePropertyConfig;
import org.hit.util.AbstractLauncher;

/**
 * Defines an application that can be used for launching the server.
 *
 * @author Balraja Subbiah
 */
public class HitServerLauncher extends AbstractLauncher
{
    private static final String SERVER_LAUNCHER_FILE = "hit_db_server";

    private static final String TRANSACTION_LOG_DIR = "transaction_log_dir";
    
    private static final String IS_MASTER = "is_master";

    private static final String WAL_BASE_DIR_PROPERTY = "org.hit.wal.basePath";

    public static void main (String[] args)
    {
        HitServerLauncher launcher = new HitServerLauncher();
        launcher.launch(args);
    }

    private final Options myServerCommandLineOptions;

    /**
     * CTOR
     */
    public HitServerLauncher()
    {
        myServerCommandLineOptions = new Options();
        myServerCommandLineOptions.addOption(
            LOG_FILE,
            LOG_FILE,
            true,
            "The complete path of server log file"
        );

        myServerCommandLineOptions.addOption(
             TRANSACTION_LOG_DIR,
             TRANSACTION_LOG_DIR,
             true,
             "The directory under which trnsaction logs are stored"
         );
        
        myServerCommandLineOptions.addOption(
            IS_MASTER,
            IS_MASTER,
            false,
            "Option to determine whether this server is marked as master");

        myServerCommandLineOptions.addOption(
             HELP,
             false,
             "Prints the help message"
         );
    }

    /** A helper method for launching server */
    public void launch(String[] args)
    {
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmdLine =
                parser.parse(myServerCommandLineOptions, args);
            StringBuilder commandBuilder = new StringBuilder();
            String applicationHome  =
                System.getProperty(APP_HOME_PROPERTY);

            if (applicationHome == null) {
                System.out.println("Application home is not defined");
                System.exit(1);
            }

            commandBuilder.append(applicationHome);
            commandBuilder.append(
                 File.separator + "bin"
                 + File.separator + SERVER_LAUNCHER_FILE
                 + getLauncherScriptSuffix());

            StringBuilder optsBuilder = new StringBuilder();
            if (cmdLine.hasOption(LOG_FILE)) {
                optsBuilder.append(addProperty(
                    LOG_FILE_PROPERTY,
                    cmdLine.getOptionValue(LOG_FILE)));
            }

            if (cmdLine.hasOption(TRANSACTION_LOG_DIR)) {
                optsBuilder.append(addProperty(
                    WAL_BASE_DIR_PROPERTY,
                    cmdLine.getOptionValue(TRANSACTION_LOG_DIR)));
            }
            
            if (cmdLine.hasOption(IS_MASTER)) {
                optsBuilder.append(addProperty(
                    NodePropertyConfig.MASTER_PROPERTY,
                    "true"));
            }

            String zooKeeperHome = System.getenv("ZOOKEEPER_INSTALL");
            if (zooKeeperHome == null) {
                System.out.println("ZooKeeper location is not defined");
                System.exit(1);
            }

            ProcessBuilder bldr = null;
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                bldr = new ProcessBuilder(Arrays.<String>asList(
                        "cmd", "/c", commandBuilder.toString()));
            }
            else {
                bldr = new ProcessBuilder(Arrays.<String>asList(
                    "bash", "-c", commandBuilder.toString()));
            }

            bldr.environment().put(CLASSPATH_PREFIX,
                                   zooKeeperHome
                                   + File.separator
                                   + "conf"
                                   + File.pathSeparator);

            if (optsBuilder.length() > 0) {
                bldr.environment().put(JAVA_OPTS, optsBuilder.toString());
            }

            System.out.println("Executing " + bldr.command());
            bldr.start();
        }
        catch (ParseException e) {
            System.out.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("start_db", myServerCommandLineOptions);
        }
        catch (IOException e) {
            System.out.println("Error occured while launching the server : "
                               + e.getMessage());
        }
    }
}
