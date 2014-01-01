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

import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hit.client.Constants;
import org.hit.facade.HitDBFacade;
import org.hit.util.Application;
import org.hit.util.ApplicationLauncher;

/**
 * The shell for interacting with the hit database.
 *
 * @author Balraja Subbiah
 */
public class ConsoleShell implements Application
{
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
            myCommand.execute(myServerFacade);
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

            try (Scanner scanner = new Scanner(System.in)) {
                System.out.print(PROMPT);
                while (!scanner.hasNextLine()){
                    // Do nothing
                }
                String line = scanner.nextLine();
                Command command = myCommandParser.parse(line);
                myCommandExecutor.submit(new CommandTask(command));
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


    /**
     * CTOR
     */
    public ConsoleShell()
    {
        super();
        myCommandExecutor = Executors.newSingleThreadExecutor();
        myCommandParser = new CommandParser();
        myServerFacade = new HitDBFacade();
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
