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

import java.io.File;

import org.hit.util.ApplicationProperties;

/**
 * Implements <code>WalConfig</code> that reads values from the <code>
 * ApplicationProperties</code>.
 * 
 * @author Balraja Subbiah
 */
public class WALPropertyConfig implements WALConfig
{
    private static final String LOCAL_WAL_DIRECTORY = "hitdb_wal";
    
    private static final String WAL_BASE_DIRECTORY = "org.hit.wal.basePath";
    
    private static final String WAL_TRANCTIONS_PER_FILE = 
        "org.hit.wal.transactionsPerFile";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getBaseDirectoryPath()
    {
        String basePath = 
            ApplicationProperties.getProperty(WAL_BASE_DIRECTORY);
        
        if (basePath == null) {
            basePath = System.getProperty("java.io.tmpdir");
            basePath += File.separator + LOCAL_WAL_DIRECTORY;
            File file = new File(basePath);
            file.mkdirs();
        }
        return basePath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTransactionsPerFile()
    {
        String transactionsPerFile = 
            ApplicationProperties.getProperty(WAL_TRANCTIONS_PER_FILE);
        return transactionsPerFile != null ? Integer.valueOf(transactionsPerFile)
                                           : -1;
    }
}
