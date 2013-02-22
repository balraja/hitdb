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

package org.hit.db.transactions.journal;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.util.LogFactory;

/**
 * The facade for accessing the local file system available on the 
 * computer.
 * 
 * @author Balraja Subbiah
 */
public class StandardFileSystem implements FileSystemFacacde
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(StandardFileSystem.class);

    /**
     * {@inheritDoc}
     */
    @Override
    public DataOutputStream createFile(String path, boolean append)
    {
        File file = new File(path);
        DataOutputStream dout = null;
        try {
           dout = new DataOutputStream(new FileOutputStream(file, append));
        }
        catch (FileNotFoundException e) {
            LOG.log(Level.SEVERE, e.getMessage(), e);
        }
        return dout;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean makeDirectory(String path)
    {
        File file = new File(path);
        return file.mkdirs();
    }
  
}
