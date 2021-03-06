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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.hit.util.AbstractLauncher;
import org.hit.util.PropertyLogConfig;

/**
 * Defines the launcher for the client.
 *
 * @author Balraja Subbiah
 */
public class ClientLauncher extends AbstractLauncher
{
    private static final String CLIENT_CLASS_NAME = "client_class_name";

    private static final String CLIENT_LAUNCHER_FILE = "hit_db_client";

    private static final String JAR_PATH = "jar_path";

    public static void main (String[] args)
    {
        ClientLauncher launcher = new ClientLauncher();
        launcher.launch(args);
    }

    private final Options myClientCommandLineOptions;
    
    private final AtomicBoolean myIsAlive;
    
    private class OutputStreamGobbler implements Runnable
    {
        private final BufferedReader myReader;
        
        /**
         * CTOR
         */
        public OutputStreamGobbler(InputStream in)
        {
            myReader = new BufferedReader(new InputStreamReader(in));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            while (myIsAlive.get()) {
                String line;
                try {
                    while((line = myReader.readLine()) != null) {
                        System.out.println(line);
                    }
                    
                }
                catch (IOException e) {
                }
            }
        }
    }

    /**
     * CTOR
     */
    public ClientLauncher()
    {
        myIsAlive = new AtomicBoolean(true);
        myClientCommandLineOptions = new Options();
        myClientCommandLineOptions.addOption(
            LOG_FILE,
            true,
            "The complete path of client log file"
        );

        myClientCommandLineOptions.addOption(
             CLIENT_CLASS_NAME,
             CLIENT_CLASS_NAME,
             true,
             "The fully qualified class name that's to be executed ");

        myClientCommandLineOptions.addOption(
             JAR_PATH,
             JAR_PATH,
             true,
             "The jar file which has the client class ");

        myClientCommandLineOptions.addOption(
             HELP,
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
                parser.parse(myClientCommandLineOptions, args);

            if (cmdLine.hasOption(HELP)) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp( "run_client", myClientCommandLineOptions);
                System.exit(0);
            }

            if (    !cmdLine.hasOption(CLIENT_CLASS_NAME)
                 || !cmdLine.hasOption(JAR_PATH))
            {
                System.out.println("Both " + "--"+ CLIENT_CLASS_NAME +
                                   " and " + "--" + JAR_PATH +
                                   " has to be specified while launching the" +
                                   " client");
                System.exit(1);
            }

            StringBuilder commandBuilder = new StringBuilder();
            String applicationHome  = System.getProperty(APP_HOME_PROPERTY);

            if (applicationHome == null) {
                System.out.println("Application home is not defined");
                System.exit(1);
            }

            commandBuilder.append(applicationHome);
            commandBuilder.append(
                 File.separator + "bin"
                 + File.separator + CLIENT_LAUNCHER_FILE
                 + getLauncherScriptSuffix());

            StringBuilder optsBuilder = new StringBuilder();
            optsBuilder.append(addProperty(
                PropertyLogConfig.LOG_FILE_NAME_PROPERTY,
                cmdLine.getOptionValue(LOG_FILE)));

            optsBuilder.append(addProperty(
                DBClientExecutor.CLIENT_CLASS_NAME_PROPERTY,
                cmdLine.getOptionValue(CLIENT_CLASS_NAME)));

            StringBuilder classPathBuilder = new StringBuilder();
            classPathBuilder.append(cmdLine.getOptionValue(JAR_PATH));

            ProcessBuilder bldr = null;
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                bldr = new ProcessBuilder(Arrays.<String>asList(
                        commandBuilder.toString()));
            }
            else {
                bldr = new ProcessBuilder(Arrays.<String>asList(
                    "bash", "-c", commandBuilder.toString()));
            }
            bldr.environment().put(CLASSPATH_PREFIX,
                                   classPathBuilder.toString());
            bldr.environment().put(JAVA_OPTS, optsBuilder.toString());
            bldr.redirectOutput(Redirect.PIPE);
            bldr.redirectError(Redirect.PIPE);
            Process p = bldr.start();
            Thread t = new Thread(new OutputStreamGobbler(p.getInputStream()));
            t.start();
            try {
                p.waitFor();
                myIsAlive.compareAndSet(true, false);
            }
            catch (InterruptedException e) {
            }
        }
        catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(CLIENT_LAUNCHER_FILE,
                                myClientCommandLineOptions);
        }
        catch (IOException e) {
            System.out.println("Error occured while launching the server : "
                               + e.getMessage());
        }
    }
}
