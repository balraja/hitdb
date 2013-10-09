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
 * Defines an annotation for defining the characteristics of a HIT table.
 * 
 * @author Balraja Subbiah
 */
public class MetaTable
{
    private String myKeyTypeName;
    
    private String myTableName;
    
    /**
     * CTOR
     */
    public MetaTable(String keyTypeName, String tableName)
    {
        super();
        myKeyTypeName = keyTypeName;
        myTableName = tableName;
    }

    /**
     * Returns the value of keyTypeName
     */
    public String getKeyTypeName()
    {
        return myKeyTypeName;
    }

    /**
     * Returns the value of tableName
     */
    public String getTableName()
    {
        return myTableName;
    }
}
