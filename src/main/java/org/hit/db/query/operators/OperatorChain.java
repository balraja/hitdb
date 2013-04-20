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

import org.hit.db.model.Database;

/**
 * Defines contract for the query operator, which supports decoration.
 * 
 * @author Balraja Subbiah
 */
public abstract class OperatorChain<Q,P> implements QueryOperator<Q>
{
    private final QueryOperator<P> myDecoratedOperator;
    
    /**
     * CTOR
     */
    public OperatorChain(QueryOperator<P> operator)
    {
        myDecoratedOperator = operator;
    }

    /**
     * {@inheritDoc} 
     */
    @Override
    public Q getResult(Database database)
    {
        return myDecoratedOperator != null ? 
           doPerformOperation(myDecoratedOperator.getResult(database))
           : null;
    }
    
    /**
     * Subclasses should override this method to perform the required 
     * translation.
     */
    protected abstract Q doPerformOperation(P toBeOperatedCollection);
}
