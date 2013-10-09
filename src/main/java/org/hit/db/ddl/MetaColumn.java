/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

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

package org.hit.db.ddl;

/**
 * Defines a type for capturing information about a column.
 * 
 * @author Balraja Subbiah
 */
public class MetaColumn
{
    private final boolean myPrimary;
    
    private final String myName;
    
    private final String myType;
    
    private final int myIndex;
    
    /**
     * CTOR
     */
    public MetaColumn(boolean primary, 
                      int     index,
                      String  name, 
                      String  type)
    {
        super();
        myPrimary = primary;
        myName = name;
        myType = type;
        myIndex = index;
    }
    
    /**
     * Returns the value of primary
     */
    public boolean isPrimary()
    {
        return myPrimary;
    }

    /**
     * Returns the value of name
     */
    public String getName()
    {
        return myName;
    }

    public String getQualifiedType()
    {
        return myType;
    }

    public String getType()
    {
        int lastIndex = myType.lastIndexOf('.');
        return  lastIndex > -1 ? myType.substring(lastIndex + 1)
                               : myType;
    }

    public int getIndex()
    {
        return myIndex;
    }
   
    public String getVariableName()
    {
        return Character.toLowerCase(myName.charAt(0)) + myName.substring(1);
    }

    public boolean isImportNecessary()
    {
        return myType.indexOf('.') > -1;
    }
}
