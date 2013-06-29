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

import org.hit.client.DBClient;
import org.hit.facade.HitDBFacade;

/**
 * The shell for interacting with the hit database.
 * 
 * @author Balraja Subbiah
 */
public class HitDBShell extends DBClient
{
    private static final String BANNER = 
        "Hit Database Shell Copyright (C) 2013 Balraja Subbiah";
    
    private static final String HELP_MSG = 
        "Use 'help' for assistance";
    
    private static final String PROMPT = ">";
    
    private HitDBFacade myFacade;
    
    private final ExecutorService myCommandExecutor;
    
    private final CommandParser myCommandParser;
    
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
            myCommand.execute(myFacade);
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
            Scanner scanner = new Scanner(System.in);
            System.out.print(PROMPT);
            while (!scanner.hasNextLine()){
                // Do nothing
            }
            String line = scanner.nextLine();
            Command command = myCommandParser.parse(line);
            myCommandExecutor.submit(new CommandTask(command));
        }
    }
    
    /**
     * CTOR
     */
    public HitDBShell()
    {
        super();
        myCommandExecutor = Executors.newSingleThreadExecutor();
        myCommandParser = new CommandParser();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown()
    {
        super.shutdown();
        myCommandExecutor.shutdownNow();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        System.out.println(BANNER);
        System.out.println(HELP_MSG);
        super.start();
        myCommandExecutor.submit(new ReaderTask());
    }
}
