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

import java.util.Set;

import org.hit.facade.HitDBFacade;

/**
 * The command to display the list of tables known to this database server.
 * 
 * @author Balraja Subbiah
 */
@MetaCommand(name = "list_tables", 
             help = "Displays the list of tables in the database")
public class ListTablesCommand implements Command
{
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(HitDBFacade facade)
    {
        Set<String> tables = facade.listTables();
        if (tables == null || tables.isEmpty()) {
            System.out.println("There are no tables to display");
        }
        else {
            int i = 1;
            for (String table : tables) {
                System.out.println(i + " " + table);
                i++;
            }
        }
    }
}
