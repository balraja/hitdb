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

package org.hit.db.model;

/**
 * Defines the contract for an exception that can be used for
 * indicating errors in database.
 * 
 * @author Balraja Subbiah
 */
public class DatabaseException extends Exception
{
    /**
     * The serial version id.
     */
    private static final long serialVersionUID = 7920543780587210755L;

    /**
     * CTOR
     */
    public DatabaseException()
    {
        super();
    }

    /**
     * CTOR
     */
    public DatabaseException(String message)
    {
        super(message);
    }

    /**
     * CTOR
     */
    public DatabaseException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * CTOR
     */
    public DatabaseException(Throwable cause)
    {
        super(cause);
    }
}
