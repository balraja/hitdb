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

package org.hit.node;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;

import org.hit.db.model.Schema;

/**
 * Defines the allocation of points on key space corresponding to the
 * new node.
 * 
 * @author Balraja Subbiah
 */
public class Allocation implements Externalizable
{
    private Map<String, Schema> myTable2SchemaMap;
    
    private Map<String, Comparable<?>> myTable2KeyMap;
    
    /**
     * CTOR
     */
    public Allocation()
    {
        myTable2KeyMap = new HashMap<>();
        myTable2SchemaMap = new HashMap<>();
    }

    /**
     * CTOR
     */
    public Allocation(Map<String, Schema> table2SchemaMap,
                      Map<String, Comparable<?>> table2KeyMap)
    {
        super();
        myTable2SchemaMap = table2SchemaMap;
        myTable2KeyMap = table2KeyMap;
    }

    /**
     * Returns the value of table2SchemaMap
     */
    public Map<String, Schema> getTable2SchemaMap()
    {
        return myTable2SchemaMap;
    }

    /**
     * Setter for the table2SchemaMap
     */
    public void setTable2SchemaMap(Map<String, Schema> table2SchemaMap)
    {
        myTable2SchemaMap = table2SchemaMap;
    }

    /**
     * Returns the value of table2KeyMap
     */
    public Map<String, Comparable<?>> getTable2KeyMap()
    {
        return myTable2KeyMap;
    }

    /**
     * Setter for the table2KeyMap
     */
    public void setTable2KeyMap(Map<String, Comparable<?>> table2KeyMap)
    {
        myTable2KeyMap = table2KeyMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myTable2KeyMap);
        out.writeObject(myTable2SchemaMap);
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
        myTable2KeyMap = (Map<String, Comparable<?>>) in.readObject();
        myTable2SchemaMap = (Map<String, Schema>) in.readObject();
    }
}
