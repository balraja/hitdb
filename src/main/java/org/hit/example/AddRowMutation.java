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

package org.hit.example;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.hit.db.model.Database;
import org.hit.db.model.Table;
import org.hit.db.model.mutations.SingleKeyMutation;

/**
 * Implements <code>SingleKeyMutation</code> for adding a new row to the
 * database.
 *
 * @author Balraja Subbiah
 */
public class AddRowMutation implements SingleKeyMutation<ClimateDataKey>
{
    private ClimateData myInsertedRow;

    /**
     * CTOR
     */
    public AddRowMutation()
    {
        this(null);
    }

    /**
     * CTOR
     */
    public AddRowMutation(ClimateData insertedRow)
    {
        super();
        myInsertedRow = insertedRow;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        AddRowMutation other = (AddRowMutation) obj;
        if (myInsertedRow == null) {
            if (other.myInsertedRow != null) {
                return false;
            }
        }
        else if (!myInsertedRow.equals(other.myInsertedRow)) {
            return false;
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ClimateDataKey getKey()
    {
        return myInsertedRow.getKey();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                 + ((myInsertedRow == null) ? 0 : myInsertedRow.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myInsertedRow = (ClimateData) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Database database)
    {
        Table<ClimateDataKey, ClimateData> climateDataTable =
            database.lookUpTable(HitDbTest.TABLE_NAME);
        climateDataTable.update(null, myInsertedRow);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myInsertedRow);
    }
}
