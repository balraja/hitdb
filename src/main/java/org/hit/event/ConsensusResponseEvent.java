/*
    Hit is a high speed transactional database for handling millions
    of updates with comfort and ease.

    Copyright (C) 2012  Balraja Subbiah

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

package org.hit.event;

import org.hit.consensus.Proposal;
import org.hit.pool.PoolConfiguration;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines the contract for an event that get's generated to publish the
 * response to a {@link ConsensusRequestEvent}.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(initialSize = 100, size = 1000)
public class ConsensusResponseEvent implements Event, Poolable
{
    private Proposal myProposal;
    
    private boolean myAccepted;
    
    /**
     * Fluent interface method for initializing the pooled object
     */
    public static ConsensusResponseEvent create(
         Proposal proposal, boolean accepted)
    {
        ConsensusResponseEvent cre = 
            PooledObjects.getInstance(ConsensusResponseEvent.class);
        cre.myProposal = proposal;
        cre.myAccepted = accepted;
        return cre;
    }

    /**
     * Returns the value of proposal
     */
    public Proposal getProposal()
    {
        return myProposal;
    }

    /**
     * Returns the value of accepted
     */
    public boolean isAccepted()
    {
        return myAccepted;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myProposal = null;
        myAccepted = false;
    }
}
