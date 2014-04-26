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

package org.hit.consensus.raft.log;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;

import java.io.DataInputStream;
import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.consensus.Proposal;
import org.hit.di.HitServerModule;
import org.hit.fs.FileSystemFacacde;
import org.hit.util.LogFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.inject.Guice;
import com.google.inject.Injector;

/**
 * Defines the contract for the write ahead logs to which mutations are
 * pesisted before they are committed. It stores
 * {@link WALConfig#getTransactionsPerFile()} number of mutations in a file.
 * Each record is stored as Double.NaN,record_size_as_int,serialized_format.
 *
 * @author Balraja Subbiah
 */
public class WAL
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(WAL.class);

    private static final char HYPHEN = '_';
    
    private static final String TRANSACTION_LOG_SUFFIX = ".transactionLog";
    
    private static final String TERM_PREFIX = "term";

    /**
     * Type for capturing the necessary information to be written into a
     * write ahead log.
     */
    public static class WALRecord implements Externalizable
    {
        private Proposal myProposal;

        private long myTermID;
        
        private long mySequenceNo;

        /**
         * CTOR
         */
        public WALRecord()
        {
            this(Long.MIN_VALUE, Long.MIN_VALUE, null);
        }

        /**
         * CTOR
         */
        public WALRecord(long termID, long sequenceNo, Proposal proposal)
        {
            super();
            myTermID = termID;
            mySequenceNo = sequenceNo;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void readExternal(ObjectInput in)
            throws IOException, ClassNotFoundException
        {
            myTermID = in.readLong();
            mySequenceNo = in.readLong();
            myProposal = (Proposal) in.readObject();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void writeExternal(ObjectOutput out) throws IOException
        {
            out.writeLong(myTermID);
            out.writeLong(mySequenceNo);
            out.writeObject(myProposal);
        }
    }
    
    private static class FileIterator implements Iterator<WALRecord>
    {
        private final ObjectInputStream myInStream;
        
        private WALRecord myWALRecord;
        
        /**
         * CTOR
         */
        public FileIterator(DataInputStream inStream) throws IOException
        {
            myInStream = new ObjectInputStream(inStream);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasNext()
        {
            try {
                myWALRecord = (WALRecord) myInStream.readObject();
            }
            catch (ClassNotFoundException | IOException e) {
                myWALRecord = null;
                return false;
            }
            return true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public WALRecord next()
        {
            return myWALRecord;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    private final WALConfig myConfig;

    private final FileSystemFacacde myFacacde;

    private final Lock myLock;

    private ObjectOutputStream myLogStream;
    
    private long myLastTermID;
    
    /**
     * CTOR
     */
    public WAL(WALConfig config)
    {
        Injector injector = Guice.createInjector(new HitServerModule());
        myFacacde = injector.getInstance(FileSystemFacacde.class);
        myLock = new ReentrantLock();
        myConfig = config;
        myFacacde.makeDirectory(myConfig.getBaseDirectoryPath());
        LOG.info("Persisting transaction logs under " 
                 + myConfig.getBaseDirectoryPath()
                 + " with " + myConfig.getTransactionsPerFile()
                 + " transactions per file to the "
                 + myFacacde.getClass().getSimpleName() 
                 + " file system");
        
        myLastTermID = -1L;
        myLogStream = null;
    }

    /**
     * Persists the given mutation to a file system. So that it can be
     * replayed in the future.
     */
    public void addProposal(long termID, long sequenceNO, Proposal proposal)
    {
        try {
            myLock.lock();
            if (myLogStream == null || myLastTermID != termID) {
                if (myLastTermID != termID) {
                    myLogStream.flush();
                    myLogStream.close();
                }
                
                String dirPath = makeTermDirectory(termID);
                myFacacde.makeDirectory(dirPath);
                myLogStream = 
                    new ObjectOutputStream(
                        myFacacde.createFile(
                            makeFileName(dirPath, sequenceNO), false));
                myLastTermID = termID;
            }
            else if (sequenceNO % myConfig.getTransactionsPerFile() == 0) {
                myLogStream.flush();
                myLogStream.close();
                myLogStream =
                    new ObjectOutputStream(
                        myFacacde.createFile(
                             makeFileName(
                                 makeTermDirectory(termID), sequenceNO), false));
            }
            myLogStream.writeObject(
                new WALRecord(sequenceNO, sequenceNO, proposal));
        }
        catch (IOException e) {
            LOG.log(Level.SEVERE,
                    "Exception when persisting a transaction to WAL",
                    e);

        }
        finally {
            myLock.unlock();
        }
    }
    
    private String makeTermDirectory(long termID)
    {
        return myConfig.getBaseDirectoryPath()
             + File.separator
             + TERM_PREFIX
             + HYPHEN
             + Long.toString(termID);
    }

    private String makeFileName(String termDirPath, long sequenceNumber)
    {
        return termDirPath
               + File.separator
               + myConfig.getLogName()
               + HYPHEN
               + Long.toString(sequenceNumber)
               + TRANSACTION_LOG_SUFFIX;
    }
    
    /**
     * Returns an iterator over the list of proposals between the
     * sequence numbers specified.
     */
    public TLongObjectMap<Proposal> readProposalsFromLog(
        long termID, final long fromSequence, final long toSequence)
    {
        try {
            
            TLongObjectMap<Proposal> proposals = new TLongObjectHashMap<>();
            String dirPath = makeTermDirectory(termID);
            long sequence = 
                (fromSequence - (fromSequence % myConfig.getTransactionsPerFile()));
            long endSequence = 
                 (toSequence 
                  + (myConfig.getTransactionsPerFile()
                         - (toSequence % myConfig.getTransactionsPerFile())));
            
            Predicate<WALRecord> predicate = new Predicate<WAL.WALRecord>() {
    
                @Override
                public boolean apply(WALRecord record)
                {
                    return record.mySequenceNo >= fromSequence
                        && record.mySequenceNo <= toSequence;
                }
            };
            
            while (sequence <= endSequence) {
                
                DataInputStream inputLogFile = 
                    myFacacde.openFileForRead(makeFileName(dirPath, sequence));
                                
                try {
                    Iterator<WAL.WALRecord> walRecordIterator = 
                        Iterators.filter(
                            new FileIterator(inputLogFile), predicate);
                    
                    while (walRecordIterator.hasNext()) {
                        WAL.WALRecord record = walRecordIterator.next();
                        proposals.put(record.mySequenceNo, record.myProposal);
                    }
                }
                catch (IOException e) {
                    LOG.log(Level.SEVERE, 
                           "Exception while reading data from"
                            + makeFileName(dirPath, sequence),
                            e);
                }
                sequence += myConfig.getTransactionsPerFile();
            }
            return proposals;
        }
        finally {
            myLock.unlock();
        }
    }
}
