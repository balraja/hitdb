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

package org.hit.di;

import org.hit.actors.EventBus;
import org.hit.communicator.NodeID;
import org.hit.consensus.UnitID;
import org.hit.consensus.raft.log.WAL;
import org.hit.db.engine.TransactionManager;
import org.hit.db.transactions.TransactableDatabase;
import org.hit.time.Clock;

import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;

/**
 * Implements <code>Provider</code> to generate the <code>Transaction
 * Manager</code>
 * 
 * @author Balraja Subbiah
 */
public class TransactionManagerProvider implements Provider<TransactionManager>
{
    private final TransactionManager myTransactionManager;
    
    /**
     * CTOR
     */
    @Inject
    public TransactionManagerProvider(TransactableDatabase database,
                                      Clock                clock,
                                      EventBus             eventBus,
                                      NodeID               serverID,
                                      @Named("ReplicationUnitID")
                                      UnitID               replicationID)
    {
        myTransactionManager = 
            new TransactionManager(database, 
                                   clock, 
                                   eventBus, 
                                   serverID,
                                   replicationID);
    }
        
    /**
     * {@inheritDoc}
     */
    @Override
    public TransactionManager get()
    {
        return myTransactionManager;
    }
}
