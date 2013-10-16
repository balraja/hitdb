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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Queryable;

/**
 * Defines the contract for {@link Queryable} which wraps it's attributes
 * in a map.
 * 
 * @author Balraja Subbiah
 */
public class QueryableMap implements Queryable
{
    private final Map<String, Object> myAttributeMap;
    
    /**
     * CTOR
     */
    public QueryableMap()
    {
        myAttributeMap = new HashMap<>();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object getFieldValue(String fieldName)
    {
        return myAttributeMap.get(fieldName);
    }
    
    /** Returns the collection of keys stored in this map */
    public Collection<String> keySet()
    {
        return myAttributeMap.keySet();
    }
    
    /** Sets the value of an attribute */
    public void setFieldValue(String fieldName, Object value)
    {
        myAttributeMap.put(fieldName, value);
    }
}
