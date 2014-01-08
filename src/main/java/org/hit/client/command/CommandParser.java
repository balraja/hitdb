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

package org.hit.client.command;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The type that can be used for parsing the {@link Command}s entered through
 * the shell.
 *
 * @author Balraja Subbiah
 */
public class CommandParser
{
    private static final String HELP = "help";

    // This is stupid way of doing things. A better way wud to be to use APT
    // and generate the help command.
    private static final Set<Class<? extends Command>> ourCommands =
        new HashSet<>();

    static {
        ourCommands.add(ListTablesCommand.class);
    }

    private final Map<String, Command> myKeywordToCommandMap;

    public CommandParser()
    {
        myKeywordToCommandMap = new HashMap<>();
        Map<String,String> command2HelpMap = new HashMap<>();

        try {
            for (Class<? extends Command> commandClass : ourCommands) {
                MetaCommand metaCommand =
                    commandClass.getAnnotation(MetaCommand.class);
                System.out.println("Meta cmd " + metaCommand);
                Command command = commandClass.newInstance();
                myKeywordToCommandMap.put(metaCommand.name(),
                                          command);
                command2HelpMap.put(metaCommand.name(),
                                    metaCommand.help());
            }
            myKeywordToCommandMap.put(HELP, new HelpCommand(command2HelpMap));
        }
        catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    /** Parses the line and returns the {@link Command} */
    public Command parse(String line)
    {
        int index = line.indexOf(" ");
        String keyword =
            index > 0 ? line.substring(0, index) : line;
        Command command = myKeywordToCommandMap.get(keyword);
        if (command != null && command instanceof ParsableCommand) {
            ParsableCommand parsable = (ParsableCommand) command;
            parsable.init(line.substring(index + 1));
        }
        return command;
    }
}
