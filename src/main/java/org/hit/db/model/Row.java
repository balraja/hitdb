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

package org.hit.db.model;

import java.util.Collection;

/**
 * Defines an abstract type that characterizes the features necessary
 * for extracting the values of individual fields from the given record.
 * 
 * @author Balraja Subbiah
 */
public interface Row
{
    /** Returns value corresponding to the given field name */
    public Object getFieldValue(String fieldName);
    
    /** Returns the name of columns whose values are present in this row */
    public Collection<String> getFieldNames();
}
