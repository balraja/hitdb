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

package org.hit.db.engine;

import org.hit.db.model.Persistable;
import org.hit.db.model.Schema;
import org.hit.db.model.Table;

/**
 * Defines the super type for <code>Table</code> that shares common methods.
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractReplicatedTable <K extends Comparable<K>,
                                               P extends Persistable<K>>
    implements Table<K,P>
{
    private final Schema mySchema;

    /**
     * CTOR
     */
    public AbstractReplicatedTable(Schema schema)
    {
        mySchema = schema;
    }

    /**
     * Returns the value of schema
     */
    @Override
    public Schema getSchema()
    {
        return mySchema;
    }
}
