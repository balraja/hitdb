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
import org.hit.consensus.paxos.PaxosProposal;
import org.hit.db.model.DBOperation;

/**
 * Defines the contract for a {@link PaxosProposal} to be used for acheiving 
 * consensus among multitude of nodes for a transaction commit.
 * 
 * @author Balraja Subbiah
 */
public class DistributedTrnProposal implements PaxosProposal
{
    private long mySequenceNumber;
    
    private UnitID myUnitID;
    
    private Map<NodeID, DBOperation> myNodeToDBOperationMap;
    
    private NodeID myClientID;
    
    /**
     * CTOR
     */
    public DistributedTrnProposal()
    {
        super();
        mySequenceNumber = -1;
        myUnitID = null;
        myNodeToDBOperationMap = null;
        myClientID = null;
    }

    /**
     * CTOR
     */
    public DistributedTrnProposal(long sequenceNumber, 
                                  UnitID unitID,
                                  Map<NodeID, DBOperation> nodeToDBOperationMap, 
                                  NodeID clientId)
    {
        super();
        mySequenceNumber = sequenceNumber;
        myUnitID = unitID;
        myNodeToDBOperationMap = nodeToDBOperationMap;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(Proposal proposal)
    {
        return proposal instanceof DistributedTrnProposal
             && ((DistributedTrnProposal) proposal).getSequenceNumber() < mySequenceNumber;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UnitID getUnitID()
    {
        return myUnitID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getSequenceNumber()
    {
        return mySequenceNumber;
    }
    
    /**
     * Returns the value of nodeToDBOperationMap
     */
    public Map<NodeID, DBOperation> getNodeToDBOperationMap()
    {
        return myNodeToDBOperationMap;
    }
    
    /**
     * Returns the value of clientID
     */
    public NodeID getClientID()
    {
        return myClientID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeLong(mySequenceNumber);
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
        mySequenceNumber = in.readLong();
        myUnitID = (UnitID) in.readObject();
        myNodeToDBOperationMap = (Map<NodeID, DBOperation>) in.readObject();
        
    }
}
