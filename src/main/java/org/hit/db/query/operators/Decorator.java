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
import java.util.Collection;

import org.hit.db.model.Database;
import org.hit.db.model.Row;
import org.hit.util.Range;

/**
 * Defines contract for the query operator, which supports decoration.
 * 
 * @author Balraja Subbiah
 */
public abstract class Decorator implements QueryOperator
{
    private QueryOperator myDecoratedOperator;
    
    /** CTOR */
    public Decorator()
    {
    }
    
    /**
     * CTOR
     */
    public Decorator(QueryOperator operator)
    {
        myDecoratedOperator = operator;
    }
    
    /**
     * Returns the value of decoratedOperator
     */
    protected QueryOperator getDecoratedOperator()
    {
        return myDecoratedOperator;
    }

    /**
     * {@inheritDoc} 
     */
    @Override
    public Collection<Row> getResult(Database database)
    {
        return myDecoratedOperator != null ? 
           doPerformOperation(myDecoratedOperator.getResult(database))
           : null;
    }
    
    /**
     * Subclasses should override this method to perform the required 
     * translation.
     */
    protected abstract Collection<Row> doPerformOperation(
        Collection<Row> toBeOperatedCollection);

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myDecoratedOperator);        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myDecoratedOperator = (QueryOperator) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>> void updateRange(Range<K> newRange)
    {
    }
}
