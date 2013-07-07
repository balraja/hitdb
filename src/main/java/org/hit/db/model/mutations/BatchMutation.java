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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.hit.db.model.Database;
import org.hit.db.model.Persistable;
import org.hit.db.model.Table;
import org.hit.util.Range;

/**
 * Defines the contract for a mutation that can be used uploading large
 * amount of rows.
 *
 * @author Balraja Subbiah
 */
public class BatchMutation<K extends Comparable<K>, P extends Persistable<K>>
    implements RangeMutation<K,P>
{
    private class PersistableComparator implements Comparator<P>
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public int compare(P o1, P o2)
        {
            return o1.primaryKey().compareTo(o2.primaryKey());
        }

    }

    private List<P> myData;

    private String myTableName;

    /**
     * CTOR
     */
    public BatchMutation()
    {
        super();
    }

    /**
     * CTOR
     */
    public BatchMutation(String tableName, List<P> data)
    {
        super();
        myTableName = tableName;
        myData = data;
        Collections.sort(myData, new PersistableComparator());
    }

    /**
     * Returns the value of data
     */
    public List<P> getData()
    {
        return myData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Range<K> getKeyRange()
    {
        return new Range<K>(myData.get(0).primaryKey(),
                            myData.get((myData.size() - 1)).primaryKey());
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
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myTableName = in.readUTF();
        myData = (List<P>) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void update(Database database)
    {
        Table<K, P> table = database.lookUpTable(myTableName);
        for (P data : myData) {
            table.update(null, data);
        }
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
