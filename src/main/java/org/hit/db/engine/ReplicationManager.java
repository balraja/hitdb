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

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.hit.communicator.NodeID;
import org.hit.db.model.Database;
import org.hit.db.model.Schema;
import org.hit.util.NamedThreadFactory;

/**
 * The type for managing the replications of a table.
 * 
 * @author Balraja Subbiah
 */
public class ReplicationManager
{
    /**
     * Task for polling the queued proposals and applying the same to the
     * replicated database.
     */
    private class ApplyReplicaProposalTask implements Runnable
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            while(true) {
                DBReplicaProposal proposal = myQuquedProposals.poll();
                proposal.getMutation().update(myDatabase);
            }
        }
    }
    
    private final Database myDatabase;
    
    private final ExecutorService myExecutorService;
    
    private final Queue<DBReplicaProposal> myQuquedProposals;
    
    private final NodeID myReplicatedDbsNode;
    
    /**
     * CTOR
     */
    public ReplicationManager(NodeID replicatedDbsNode)
    {
        myReplicatedDbsNode = replicatedDbsNode;
        myDatabase = new ReplicatedDatabase();
        myQuquedProposals = new ConcurrentLinkedQueue<>();
        myExecutorService =
            Executors.newSingleThreadExecutor(
                new NamedThreadFactory(ReplicationManager.class));
    }
    
    public void applyReplicationProposal(DBReplicaProposal replicaProposal)
    {
        myQuquedProposals.offer(replicaProposal);
    }
    
    public void createTable(Schema schema)
    {
        myDatabase.createTable(schema);
    }
    
    /**
     * Returns the value of replicatedDbsNode
     */
    public NodeID getReplicatedDbsNode()
    {
        return myReplicatedDbsNode;
    }
    
    public void start()
    {
        myExecutorService.submit(new ApplyReplicaProposalTask());
    }

    public void stop()
    {
        myExecutorService.shutdownNow();
    }
    
    
}
