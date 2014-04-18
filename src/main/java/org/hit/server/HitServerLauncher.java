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
import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.hit.consensus.raft.log.WALPropertyConfig;
import org.hit.di.HitModule;
import org.hit.util.AbstractLauncher;
import org.hit.util.PropertyLogConfig;

/**
 * Defines an application that can be used for launching the server.
 *
 * @author Balraja Subbiah
 */
public class HitServerLauncher extends AbstractLauncher
{
    private static final String IS_MASTER = "is_master";

    private static final String SERVER_LAUNCHER_FILE = "hit_db_server";

    private static final String TRANSACTION_LOG_DIR = "transaction_log_dir";
    
    private static final String SERVER_PORT = "server_port";
    
    private static final String SERVER_NAME = "server_name";
    
    private static final String SERVER_COUNT = "server_count";
    
    private static final String REPLICATION_FACTOR = "replication_factor";
    
    private static final String REPLICATION_SLAVE_FOR = "replication_slave_for";
    
    private static final String ZOOKEEPER_CONFIG = "zookeeper_config";
    
    private static final String LOG_GC = "log_gc";
    
    private static final String GCOPTS = " -Xloggc:%s -XX:+PrintGCDetails ";
    
    private static final String GC_SUFFIX = ".gc";

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
            LOG_GC,
            false,
            "Logs the garbage collection statsitics to <LOG_FILE>.gc");


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
             SERVER_PORT,
             SERVER_PORT,
             true,
             "The port to which server bounds itself");
        
        myServerCommandLineOptions.addOption(
            SERVER_NAME,
            SERVER_NAME,
            true,
            "The name of server in the topology");
        
        myServerCommandLineOptions.addOption(
            SERVER_COUNT,
            SERVER_COUNT,
            true,
            "The number of servers launched under a topology");
        
        myServerCommandLineOptions.addOption(
            REPLICATION_SLAVE_FOR,
            REPLICATION_SLAVE_FOR,
            true,
            "The name of server for which this server acts " +
            "as a replication salve");
        
        myServerCommandLineOptions.addOption(
            REPLICATION_FACTOR,
            REPLICATION_FACTOR,
            true,
            "The number of servers to which data has to be replicated");
        
        myServerCommandLineOptions.addOption(
             HELP,
             false,
             "Prints the help message");
        
        myServerCommandLineOptions.addOption(
            DUMP_FILE,
            DUMP_FILE,
            true,
            "The file to which output and error streams of server nodes"
            + " to be written");
        
        myServerCommandLineOptions.addOption(
            ZOOKEEPER_CONFIG,
            ZOOKEEPER_CONFIG,
            true,
            "The config directory to which data is to be sent");
    }

    /** A helper method for launching server */
    public void launch(String[] args)
    {
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmdLine =
                parser.parse(myServerCommandLineOptions, args);
            StringBuilder commandBuilder = new StringBuilder();
            String applicationHome  = System.getProperty(APP_HOME_PROPERTY);

            if (applicationHome == null) {
                System.out.println("Application home is not defined");
                System.exit(1);
            }
            
            if (cmdLine.hasOption(HELP)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp("start_server", myServerCommandLineOptions);
                return;
            }

            commandBuilder.append(applicationHome);
            commandBuilder.append(
                 File.separator + "bin"
                 + File.separator + SERVER_LAUNCHER_FILE
                 + getLauncherScriptSuffix());

            StringBuilder optsBuilder = new StringBuilder();
            if (cmdLine.hasOption(LOG_FILE)) {
                optsBuilder.append(addProperty(
                    PropertyLogConfig.LOG_FILE_NAME_PROPERTY,
                    cmdLine.getOptionValue(LOG_FILE)));
            }
            
            if (cmdLine.hasOption(LOG_GC)) {
                optsBuilder.append(String.format(GCOPTS, LOG_FILE + GC_SUFFIX));
            }

            if (cmdLine.hasOption(TRANSACTION_LOG_DIR)) {
                optsBuilder.append(addProperty(
                    WALPropertyConfig.WAL_BASE_DIRECTORY,
                    cmdLine.getOptionValue(TRANSACTION_LOG_DIR)));
            }

            if (cmdLine.hasOption(IS_MASTER)) {
                optsBuilder.append(addProperty(
                    ServerPropertyConfig.IS_MASTER_PROPERTY,
                    "true"));
            }
            
            if (cmdLine.hasOption(SERVER_PORT)) {
                optsBuilder.append(addProperty(
                    HitModule.HIT_COMM_PORT_PROPERTY, 
                    cmdLine.getOptionValue(SERVER_PORT)));
            }
            else {
                throw new RuntimeException(SERVER_PORT + " not specified ");
            }
            
            if (cmdLine.hasOption(SERVER_NAME)) {
                optsBuilder.append(addProperty(
                    ServerPropertyConfig.SERVER_NAME_PROPERTY, 
                    cmdLine.getOptionValue(SERVER_NAME)));
            }
            else {
                throw new RuntimeException(SERVER_NAME + " not specified");
            }
            
            if (cmdLine.hasOption(SERVER_COUNT)) {
                optsBuilder.append(addProperty(
                    ServerPropertyConfig.SERVER_COUNT_PROPERTY, 
                    cmdLine.getOptionValue(SERVER_COUNT)));
            }
            else {
                throw new RuntimeException(SERVER_COUNT + " not specified");
            }
            
            if (cmdLine.hasOption(REPLICATION_FACTOR)) {
                optsBuilder.append(addProperty(
                    ServerPropertyConfig.REPLICATION_FACTOR_PROPERTY,
                    cmdLine.getOptionValue(REPLICATION_FACTOR)));
            }
            else {
                throw new RuntimeException(REPLICATION_FACTOR + " not specified");
            }
            
            if (cmdLine.hasOption(REPLICATION_SLAVE_FOR)) {
                optsBuilder.append(addProperty(
                    ServerPropertyConfig.REPLICATION_SLAVE_FOR_PROPERTY, 
                    cmdLine.getOptionValue(REPLICATION_SLAVE_FOR)));
            }
            else {
                throw new RuntimeException(
                    REPLICATION_SLAVE_FOR + " not specified ");
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
            
            if (optsBuilder.length() > 0) {
                bldr.environment().put(JAVA_OPTS, optsBuilder.toString());
            }
            
            if (cmdLine.hasOption(DUMP_FILE)) {
                File file = new File(cmdLine.getOptionValue(DUMP_FILE));
                bldr.redirectOutput(Redirect.to(file));
                bldr.redirectError(Redirect.to(file));
            }
            
            if (cmdLine.hasOption(ZOOKEEPER_CONFIG)) {
                StringBuilder classPathBuilder = new StringBuilder();
                classPathBuilder.append(cmdLine.getOptionValue(ZOOKEEPER_CONFIG));
                bldr.environment().put(CLASSPATH_PREFIX,
                                       classPathBuilder.toString());
            }

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
