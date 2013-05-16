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

package org.hit.facade;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.db.model.Database;
import org.hit.db.model.Query;

/**
 * Wraps the <code>Query</code> to keep track of the operation identifier.
 * 
 * @author Balraja Subbiah
 */
public class WrappedQuery implements Query
{
    private Query myQuery;
    
    private long myOperationId;
    
    /**
     * CTOR
     */
    public WrappedQuery()
    {
        this(null, -1);
    }

    /**
     * CTOR
     */
    public WrappedQuery(Query query, long operationId)
    {
        myQuery = query;
        myOperationId = operationId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myQuery);
        out.writeLong(myOperationId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myQuery = (Query) in.readObject();
        myOperationId = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object query(Database database)
    {
        return myQuery.query(database);
    }

    /**
     * Returns the value of query
     */
    public Query getQuery()
    {
        return myQuery;
    }

    /**
     * Returns the value of operationId
     */
    public long getOperationId()
    {
        return myOperationId;
    }
}
