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

import java.util.ArrayList;
import java.util.List;

import org.hit.db.model.Persistable;

/**
 * Defines the contract for a factory that's responsible for generating 
 * mutations for a particular persistable type.
 * 
 * @author Balraja Subbiah
 */
public class MutationFactory<K extends Comparable<K>, 
                             P extends Persistable<K>>
{
    private final String myTableName;
    
    /**
     * CTOR
     */
    public MutationFactory(String tableName)
    {
        super();
        myTableName = tableName;
    }
    
    /**
     * A helper method to create a {@link RangeMutation} for adding 
     * a list of data to the database.
     */
    @SuppressWarnings("unchecked")
    public RangeMutation<K,P> makeBatchAddMutation(List<? extends Object> data)
    {
        ArrayList<P> wrappedData = new ArrayList<>();
        for (Object row : data) {
            wrappedData.add((P) row);
        }
        return new BatchAddMutation<>(myTableName, wrappedData);
    }
}
