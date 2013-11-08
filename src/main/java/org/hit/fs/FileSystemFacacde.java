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

package org.hit.fs;

import java.io.DataOutputStream;

/**
 * Defines the facade that can be used for accessing the file system.
 * The main advantage of this approach is that we can easily plugin
 * different types of file system under which we wish to store the
 * transactions.
 * 
 * @author Balraja Subbiah
 */
public interface FileSystemFacacde
{
    /**
     * Creates file in append/write modes. Returns null if file creation is
     * unsuccessful.
     */
    public DataOutputStream createFile(String path, boolean append);
    
    /**
     * Returns true if the call to create a directory with the given path is
     * successful. The call to create a directory my fail if there already
     * exists a directory with the same name.
     */
    public boolean makeDirectory(String path);
    
}
