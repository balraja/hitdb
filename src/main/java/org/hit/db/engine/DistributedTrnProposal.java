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
import java.util.HashMap;
import java.util.Map;

import org.hit.communicator.NodeID;
import org.hit.consensus.Proposal;
import org.hit.consensus.UnitID;
import org.hit.db.model.DBOperation;
import org.hit.pool.PoolUtils;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines the contract for a {@link PaxosProposal} to be used for achieving 
 * consensus among multitude of nodes for a transaction commit.
 * 
 * @author Balraja Subbiah
 */
public class DistributedTrnProposal implements Proposal,Poolable
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
        myNodeToDBOperationMap = new HashMap<NodeID, DBOperation>();
        myTransactionNumber = -1L;
    }

    /**
     * CTOR
     */
    public static DistributedTrnProposal create(
        UnitID unitID,
        Map<NodeID, DBOperation> nodeToDBOperationMap, 
        long transactionNumber)
    {
        DistributedTrnProposal proposal = 
            PooledObjects.getInstance(DistributedTrnProposal.class);
        proposal.myUnitID = unitID;
        proposal.myNodeToDBOperationMap.putAll(nodeToDBOperationMap);
        proposal.myTransactionNumber = transactionNumber;
        return proposal;
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

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + (int) (myTransactionNumber ^ (myTransactionNumber >>> 32));
        result = prime * result
                + ((myUnitID == null) ? 0 : myUnitID.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DistributedTrnProposal other = (DistributedTrnProposal) obj;
        if (myTransactionNumber != other.myTransactionNumber)
            return false;
        if (myUnitID == null) {
            if (other.myUnitID != null)
                return false;
        }
        else if (!myUnitID.equals(other.myUnitID))
            return false;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myTransactionNumber = Long.MIN_VALUE;
        myUnitID = null;
        for (DBOperation operation : myNodeToDBOperationMap.values()) {
            PoolUtils.free(operation);
        }
        myNodeToDBOperationMap.clear();
    }  
}
