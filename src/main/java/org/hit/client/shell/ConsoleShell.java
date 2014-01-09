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

package org.hit.client.shell;

import java.io.Console;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.hit.client.Constants;
import org.hit.client.command.Command;
import org.hit.client.command.CommandParser;
import org.hit.facade.HitDBFacade;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;
import org.hit.util.LogFactory;

/**
 * The shell for interacting with the hit database.
 *
 * @author Balraja Subbiah
 */
public class ConsoleShell implements Application
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(ConsoleShell.class);
            
    private class CommandTask implements Runnable
    {
        private final Command myCommand;

        /**
         * CTOR
         */
        public CommandTask(Command command)
        {
            super();
            myCommand = command;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            LOG.info("Executing command " + myCommand.getClass().getSimpleName());
            
            myCommand.execute(myServerFacade, myDisplay);
            myCommandExecutor.submit(new ReaderTask());
        }
    }

    private class ReaderTask implements Runnable
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            Console c = System.console();
            while(true) {
                String line = c.readLine(PROMPT);
                Command command = myCommandParser.parse(line);
                if (command != null) {
                    myCommandExecutor.submit(new CommandTask(command));
                    break;
                }
                else {
                    System.out.println("UNABLE TO PARSE " + line);
                }
            }
        }
    }

    public static final String PROMPT = ">";

    public static void main(String[] args)
    {
        ApplicationLauncher launcher =
            new ApplicationLauncher(new ConsoleShell());
        launcher.launch();
    }

    private final ExecutorService myCommandExecutor;

    private final CommandParser myCommandParser;

    private final  HitDBFacade myServerFacade;
    
    private final ConsoleDisplay myDisplay;

    /**
     * CTOR
     */
    public ConsoleShell()
    {
        super();
        myCommandExecutor = Executors.newSingleThreadExecutor();
        myCommandParser = new CommandParser();
        myServerFacade = new HitDBFacade();
        myDisplay = new ConsoleDisplay();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        myServerFacade.start();
        System.out.println(Constants.BANNER);
        System.out.println(Constants.HELP_MSG);
        myCommandExecutor.submit(new ReaderTask());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        myCommandExecutor.shutdownNow();
        myServerFacade.stop();
    }
}
