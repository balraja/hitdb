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

package org.hit.key;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.key.domain.DiscreteDomain;
import org.hit.partitioner.Partitioner;

/**
 * Defines the contract for the key space that's partitioned between the
 * nodes in a linear fashion.
 *
 * @author Balraja Subbiah
 */
public class LinearKeyspace<S extends Comparable<S>>
    implements Keyspace<S,S>
{
    private DiscreteDomain<S> myDomain;

    /**
     * CTOR
     */
    public LinearKeyspace()
    {
        myDomain = null;
    }

    /**
     * CTOR
     */
    public LinearKeyspace(DiscreteDomain<S> domain)
    {
        myDomain = domain;
    }

    /**
     * Returns the possible enumerations of a key.
     */
    public DiscreteDomain<S> getDomain()
    {
        return myDomain;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myDomain = (DiscreteDomain<S>) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myDomain);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public S map(S key)
    {
        return key;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Partitioner<S, S> makePartitioner(String tableName)
    {
        return new Partitioner<>(tableName, this);
    }
}
