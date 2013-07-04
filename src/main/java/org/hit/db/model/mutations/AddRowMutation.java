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

package org.hit.db.model.mutations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.db.model.Database;
import org.hit.db.model.Persistable;
import org.hit.db.model.Table;

/**
 * A mutation to support adding a new row to a table.
 *
 * @author Balraja Subbiah
 */
public class AddRowMutation<K extends Comparable<K>, P extends Persistable<K>>
    implements SingleKeyMutation<K>
{
    private P myData;

    private String myTableName;

    /**
     * CTOR
     */
    public AddRowMutation()
    {
        super();
    }

    /**
     * CTOR
     */
    public AddRowMutation(P data, String tableName)
    {
        super();
        myData = data;
        myTableName = tableName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public K getKey()
    {
        return myData.primaryKey();
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
        myTableName = in.readUTF();
        myData = (P) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Database database)
    {
        Table<K, P> table = database.lookUpTable(myTableName);
        table.update(null, myData);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myTableName);
        out.writeObject(myData);
    }
}
