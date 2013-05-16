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

package org.hit.db.query.merger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.hit.db.model.Queryable;

/**
 * A simple query merger which just collects the partial result and returns
 * the final collected value.
 * 
 * @author Balraja Subbiah
 */
public class SimpleQueryMerger implements QueryMerger
{
    private final List<Queryable> myCollectedResult;
    
    /**
     * CTOR
     */
    public SimpleQueryMerger()
    {
        super();
        myCollectedResult = new ArrayList<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addPartialResult(Collection<Queryable> result)
    {
        myCollectedResult.addAll(result);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Queryable> getMergedResult()
    {
        return myCollectedResult;
    }
}
