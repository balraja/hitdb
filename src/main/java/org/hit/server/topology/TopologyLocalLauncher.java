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
package org.hit.server.topology;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.configuration.ConfigurationException;
import org.hit.util.AbstractLauncher;

/**
 * Launches the servers specified in the given topology on the local machine.
 * 
 * @author Balraja Subbiah
 */
public final class TopologyLocalLauncher extends AbstractLauncher
{
    
    private static final String APP_HOME_PROPERTY = "org.hit.app.home";
    
    private static final String TOPOLOGY_FILE = "topology_file";
    
    private static final String LOG_DIR = "log_directory";
    
    private static final String SERVER_LAUNCHER_COMMAND = "start_server ";
    
    private static final String SERVER_NAME = " --server_name ";
    
    private static final String SERVER_PORT = " --server_port ";
    
    private static final String SERVER_COUNT = " --server_count ";
    
    private static final String IS_MASTER = " --is_master ";
    
    private static final String REPLICAION_FACTOR = " --replication_factor ";
    
    private static final String REPLICATION_SLAVE_FOR = " --replication_slave_for ";
    
    private static final String LOG_FILE = " --log_file ";
    
    private static final String LOG_SUFFIX = ".log";
    
    private final Options myServerCommandLineOptions;

    /**
     * CTOR
     */
    public TopologyLocalLauncher()
    {
        super();
        myServerCommandLineOptions = new Options();
        myServerCommandLineOptions.addOption(
            TOPOLOGY_FILE, 
            true, 
            "The topology file that specifies the servers");
        
        myServerCommandLineOptions.addOption(
            LOG_DIR, 
            true, 
            "Specifies the directory under which server's logs are stored");
       
    }
    
    /**
     * Parses command line arguments and launches the servers specified
     * in the topology file.
     */
    public void launch(String[] args)
    {
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cmdLine =
                parser.parse(myServerCommandLineOptions, args);
            
            String topologyFile = 
                cmdLine.getOptionValue(TOPOLOGY_FILE);
            
            if (topologyFile == null) {
                System.out.println(TOPOLOGY_FILE + " is mandatory.");
                System.exit(0);
            }
            
            String logDirectory = cmdLine.getOptionValue(LOG_DIR);
            if (logDirectory == null) {
                System.out.println(LOG_DIR + " is a mandatory option");
                System.exit(0);
            }
            
            String applicationHome = System.getProperty(APP_HOME_PROPERTY);

            if (applicationHome == null) {
                System.out.println("Application home is not defined");
                System.exit(1);
            }
                
            
            ServerTopology topology = new ServerTopology(new File(topologyFile));
            List<String> servers = topology.getServers();
            int port = 9001;
            for (String server : servers) {
                String command = 
                    buildServerCommand(applicationHome,
                                       server, 
                                       port++, 
                                       servers.size(), 
                                       topology.isMasterServer(server), 
                                       topology.getIamReplicaFor(server),
                                       logDirectory);
                runCommand(command);
                    
            }
        }
        catch(ParseException | ConfigurationException e)
        {
            e.printStackTrace();
        }
    }
    
    private String buildServerCommand(
        String appHome,
        String serverName,
        int serverPort,
        int serverCount,
        boolean isMaster,
        String replicaFor,
        String baseLogDir)
    {
        StringBuilder commandBuilder = new StringBuilder();
        commandBuilder.append(appHome);
        commandBuilder.append(
               File.separator 
             + BIN_DIRECTORY
             + File.separator 
             + SERVER_LAUNCHER_COMMAND
             + getLauncherScriptSuffix());
        
        commandBuilder.append(SERVER_NAME + serverName);
        commandBuilder.append(SERVER_PORT + serverPort);
        
        commandBuilder.append(SERVER_COUNT + serverCount);
        if (isMaster) {
            commandBuilder.append(IS_MASTER);
        }
        commandBuilder.append(REPLICAION_FACTOR + 1);
        commandBuilder.append(REPLICATION_SLAVE_FOR + replicaFor);
        
        commandBuilder.append(LOG_FILE 
                             + baseLogDir
                             + File.separator
                             + serverName
                             + LOG_SUFFIX);
        
        return commandBuilder.toString();
    }
    
    protected void runCommand(String command)
    {
        ProcessBuilder bldr = null;
        if (System.getProperty("os.name").toLowerCase().contains("win")) {
            bldr = new ProcessBuilder(Arrays.<String>asList(
                    "cmd", "/c", command));
        }
        else {
            bldr = new ProcessBuilder(Arrays.<String>asList(
                "bash", "-c", command));
        }
        try {
            bldr.start();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
    
}
