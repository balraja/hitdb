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

import org.hit.db.model.Query;
import org.hit.db.model.query.RewritableQuery;
import org.hit.db.query.merger.QueryMerger;
import org.hit.util.Range;

/**
 * Extends {@link QueryAdaptor} to support implementing {@link RewritableQuery}.
 * 
 * @author Balraja Subbiah
 */
public class RewritableQueryAdapter extends QueryAdaptor 
    implements RewritableQuery
{
    private QueryMerger myQueryMerger;
    
    /**
     * CTOR
     */
    public RewritableQueryAdapter()
    {
        this(null, null);
    }

    /**
     * CTOR
     */
    public RewritableQueryAdapter(QueryAdaptor query,
                                  QueryMerger  queryMerger)
    {
        super(query.getOperator());
        myQueryMerger = queryMerger;
    }
    
    /**
     * CTOR
     */
    public RewritableQueryAdapter(QueryOperator query,
                                  QueryMerger  queryMerger)
    {
        super(query);
        myQueryMerger = queryMerger;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public RewritableQuery cloneQuery()
    {
        return new RewritableQueryAdapter(getOperator(), myQueryMerger);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>> void updateRange(Range<K> newRange)
    {
        getOperator().updateRange(newRange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryMerger getQueryMerger()
    {
        return myQueryMerger;
    }
}
