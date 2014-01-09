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

import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.antlr.runtime.RecognitionException;
import org.hit.db.sql.operators.QueryBuildingException;
import org.hit.facade.HitDBFacade;
import org.hit.facade.QueryResponse;
import org.hit.util.LogFactory;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Command to display the query result in a table. This command can be 
 * invoked as follows:
 * 
 * </br></br>
 * 
 * query 'select * from table'.
 * 
 * @author Balraja Subbiah
 */
@MetaCommand(name = "query", 
             help = "Executes the sql query against the database")
public class QueryCommand implements ParsableCommand
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(QueryCommand.class);
            
    private String myQuery;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(HitDBFacade facade, Display display)
    {
        try {
            ListenableFuture<QueryResponse> resultFuture = 
                facade.queryDB(myQuery);
            QueryResponse response = resultFuture.get();
            display.publishRows(myQuery, response.getQueryResponse());
        }
        catch (QueryBuildingException 
                | RecognitionException 
                | InterruptedException 
                | ExecutionException e) 
        {
            LOG.log(Level.SEVERE, e.getMessage(), e);
            display.publishError(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(String arguments)
    {
        myQuery = arguments.substring(arguments.indexOf("'") + 1, 
                                      arguments.length() - 1);
        LOG.info("Initialized with query [ " + myQuery + " ]");
    }
}
