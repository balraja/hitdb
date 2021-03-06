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

/**
 * Defines an marker interface for the predicates that can be used for 
 * querying data in a database.
 * 
 * @author Balraja Subbiah
 */
public interface Predicate
{
    public static final Predicate MATCH_ALL = 
        new Predicate() {
            @Override
            public boolean isInterested(Row row)
            {
                return true;
            }
        };
    
    public boolean isInterested(Row row);
}
