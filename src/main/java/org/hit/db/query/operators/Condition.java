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

import java.io.Externalizable;

import org.hit.db.model.Row;
import org.hit.util.Range;

/**
 * Defines the contract for the condition used for matching against 
 * the attributes of the object.
 * 
 * @author Balraja Subbiah
 */
public interface Condition extends Externalizable
{
    /** Returns true if the condition holds on the given record */
    boolean isValid(Row record);
    
    /** Updates the filtering condition to use the new range */
    <K extends Comparable<K>> void updateRange(Range<K> newRange);
    
    /** Defines the contract for cloning  this object*/
    public Condition cloneCondition();
}
