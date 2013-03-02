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

package org.hit.facade;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.db.model.Database;
import org.hit.db.model.Mutation;

/**
 * Defines contract for a class that wraps the given <code>Mutation</code>
 * with an identifier assigned by the facade.
 *
 * @author Balraja Subbiah
 */
public class WrappedMutation implements Mutation
{
    private long myID;

    private Mutation myMutation;

    /**
     * CTOR
     */
    public WrappedMutation()
    {
        this(-1, null);
    }

    /**
     * CTOR
     */
    public WrappedMutation(long id, Mutation mutation)
    {
        super();
        myID = id;
        myMutation = mutation;
    }

    /**
     * Returns the value of iD
     */
    public long getID()
    {
        return myID;
    }

    /**
     * Returns the value of mutation
     */
    public Mutation getMutation()
    {
        return myMutation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myID = in.readLong();
        myMutation = (Mutation) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Database database)
    {
        myMutation.update(database);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(myID);
        out.writeObject(myMutation);

    }
}
