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

import java.util.Map;

import org.hit.facade.HitDBFacade;

/**
 * Implements the {@link Command} to display the help message.
 * 
 * @author Balraja Subbiah
 */
public class HelpCommand implements Command
{
    private final Map<String, String> myCommand2HelpMap;

    /**
     * CTOR
     */
    public HelpCommand(Map<String,String> command2Help)
    {
        myCommand2HelpMap = command2Help;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(HitDBFacade facade)
    {
        for (Map.Entry<String,String> entry : myCommand2HelpMap.entrySet())
        {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
