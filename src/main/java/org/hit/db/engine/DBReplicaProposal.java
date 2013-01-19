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

package org.hit.db.engine;

import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.consensus.paxos.PaxosProposal;
import org.hit.db.model.Mutation;

/**
 * Defines the contract for a proposal for replicating the databases.
 * 
 * @author Balraja Subbiah
 */
public class DBReplicaProposal implements PaxosProposal
{
    private final long myTransactionID;
    
    private final Mutation myMutation;
    
    private final UnitID myConsensusUnitID;

    /**
     * CTOR
     */
    public DBReplicaProposal(
        long transactionID, Mutation mutation, UnitID consensusUnitID)
    {
        super();
        myTransactionID = transactionID;
        myMutation = mutation;
        myConsensusUnitID = consensusUnitID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Proposal proposal)
    {
        if (proposal instanceof DBReplicaProposal) {
            DBReplicaProposal otherProposal = (DBReplicaProposal) proposal;
            return otherProposal.getSequenceNumber() < myTransactionID;
        }
        return false;
    }

    /**
     * Returns the value of mutation
     */
    public Mutation getMutation()
    {
        return myMutation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getSequenceNumber()
    {
        return myTransactionID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UnitID getUnitID()
    {
        return myConsensusUnitID;
    }
}
