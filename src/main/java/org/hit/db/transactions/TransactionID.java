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

package org.hit.db.transactions;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * The type that encapsulates the unique id assigned to a transaction.
 *
 * @author Balraja Subbiah
 */
public class TransactionID implements Externalizable,Comparable<TransactionID>
{
    private long myIdentifier;

    /**
     * CTOR
     */
    public TransactionID()
    {
        this(-1L);
    }

    /**
     * CTOR
     */
    public TransactionID(long identifier)
    {
        myIdentifier = identifier;
    }

    /**
     * Returns the value of identifier
     */
    public long getIdentifier()
    {
        return myIdentifier;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myIdentifier = in.readLong();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(myIdentifier);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TransactionID o)
    {
        return Long.compare(myIdentifier, o.getIdentifier());
    }
}
