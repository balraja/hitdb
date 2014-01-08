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

package org.hit.db.sql.operators;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;

import org.hit.db.model.Row;

/**
 * Defines the contract for the query operator that supports filtering
 * on the aggregate values of a group.
 * 
 * @author Balraja Subbiah
 */
public class Having extends Decorator
{
    private Condition myCondition;
    
    /**
     * CTOR
     */
    public Having()
    {
        myCondition = null;
    }
    
    /**
     * CTOR
     */
    public Having(QueryOperator grouper, Condition condition)
    {
        super(grouper);
        myCondition = condition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<Row>
        doPerformOperation(Collection<Row> toBeOperatedCollection)
    {
        ArrayList<Row> result = new ArrayList<>();
        for (Row q : toBeOperatedCollection) {
            if (myCondition.isValid(q)) {
                result.add(q);
            }
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myCondition);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myCondition = (Condition) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryOperator cloneOperator()
    {
        return new Having(getDecoratedOperator().cloneOperator(),
                          myCondition.cloneCondition());
    }
}
