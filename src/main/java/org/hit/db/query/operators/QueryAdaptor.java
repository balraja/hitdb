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

package org.hit.db.query.operators;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.db.model.Database;
import org.hit.db.model.Query;

/**
 * Defines a wrapper on top of {@link QueryOperator} to match 
 * the general {@link Query} interface.
 * 
 * @author Balraja Subbiah
 */
public class QueryAdaptor implements Query
{
    private QueryOperator myQueryOperator;
    
    /**
     * CTOR
     */
    public QueryAdaptor()
    {
        myQueryOperator = null;
    }

    /**
     * CTOR
     */
    public QueryAdaptor(QueryOperator queryOperator)
    {
        super();
        myQueryOperator = queryOperator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myQueryOperator);        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myQueryOperator = (QueryOperator) in.readObject();        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object query(Database database)
    {
        return myQueryOperator.getResult(database);
    }
    
    protected QueryOperator getOperator()
    {
        return myQueryOperator;
    }
}
