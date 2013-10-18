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

package org.hit.db.query.merger;

import java.util.Collection;

import org.hit.db.model.Row;

/**
 * Defines an interface for merging the query results from different nodes
 * 
 * @author Balraja Subbiah
 */
public interface QueryMerger
{
    /** Adds partial result from individual nodes */
    public void addPartialResult(Collection<Row> result);
    
    /** The final result of query after merging the values */
    public Collection<Row> getMergedResult();
}
