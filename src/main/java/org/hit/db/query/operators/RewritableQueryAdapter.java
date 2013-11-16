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
import org.hit.util.Range;

/**
 * Extends {@link QueryAdaptor} to support implementing {@link RewritableQuery}.
 * 
 * @author Balraja Subbiah
 */
public class RewritableQueryAdapter extends QueryAdaptor 
    implements RewritableQuery
{
    private String myTableName;
    
    private Range<?> myQueryRange;

    /**
     * CTOR
     */
    public RewritableQueryAdapter()
    {
        this(null, null, null);
    }

    /**
     * CTOR
     */
    public RewritableQueryAdapter(QueryOperator operator,
                                  String        tableName,
                                  Range<?>      range)
    {
        super(operator);
        myTableName  = tableName;
        myQueryRange = range;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <K extends Comparable<K>> Range<K> getRange()
    {
        return (Range<K>) myQueryRange;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTableName()
    {
        return myTableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeUTF(myTableName);
        out.writeObject(myQueryRange);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myTableName = in.readUTF();
        myQueryRange = (Range<?>) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>> Query updateRange(Range<K> newRange)
    {
        QueryOperator newOperator = 
            getOperator().cloneOperator();
        newOperator.updateRange(newRange);
        return new QueryAdaptor(newOperator);
    }
}
