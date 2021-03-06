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

/**
 * Implements {@link GroupKey} to support aggregating columns in the 
 * select clause.
 * 
 * @author Balraja Subbiah
 */
public class SelectAggregateKey implements GroupKey
{
    private String myTableName;
    
    /**
     * CTOR
     */
    public SelectAggregateKey()
    {
        this(null);
        
    }
    
    /**
     * CTOR
     */
    public SelectAggregateKey(String tableName)
    {
        super();
        myTableName = tableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myTableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        myTableName = in.readUTF();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime
                * result
                + ((myTableName == null) ? 0 : myTableName
                        .hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SelectAggregateKey other = (SelectAggregateKey) obj;
        if (myTableName == null) {
            if (other.myTableName != null)
                return false;
        }
        else if (!myTableName.equals(other.myTableName))
            return false;
        return true;
    }
 }
