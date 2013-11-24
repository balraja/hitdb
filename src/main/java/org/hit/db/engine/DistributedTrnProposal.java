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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;

import org.hit.communicator.NodeID;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.db.model.DBOperation;

/**
 * Defines the contract for a {@link PaxosProposal} to be used for achieving 
 * consensus among multitude of nodes for a transaction commit.
 * 
 * @author Balraja Subbiah
 */
public class DistributedTrnProposal implements Proposal
{
    private long myTransactionNumber;
    
    private UnitID myUnitID;
    
    private Map<NodeID, DBOperation> myNodeToDBOperationMap;
    
    /**
     * CTOR
     */
    public DistributedTrnProposal()
    {
        super();
        myUnitID = null;
        myNodeToDBOperationMap = null;
        myTransactionNumber = -1L;
    }

    /**
     * CTOR
     */
    public DistributedTrnProposal(UnitID unitID,
                                  Map<NodeID, DBOperation> nodeToDBOperationMap, 
                                  long transactionNumber)
    {
        super();
        myUnitID = unitID;
        myNodeToDBOperationMap = nodeToDBOperationMap;
        myTransactionNumber = transactionNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UnitID getUnitID()
    {
        return myUnitID;
    }

    public long getTransactionNumber()
    {
        return myTransactionNumber;
    }
    
    /**
     * Returns the value of nodeToDBOperationMap
     */
    public Map<NodeID, DBOperation> getNodeToDBOperationMap()
    {
        return myNodeToDBOperationMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(myTransactionNumber);
        out.writeObject(myUnitID);
        out.writeObject(myNodeToDBOperationMap);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myTransactionNumber = in.readLong();
        myUnitID = (UnitID) in.readObject();
        myNodeToDBOperationMap = (Map<NodeID, DBOperation>) in.readObject();
        
    }
}
