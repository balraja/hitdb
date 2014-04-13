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

package org.hit.event;

import org.hit.pool.PoolConfiguration;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/**
 * Defines an event that's specifies whether the proposal received via
 * consensus protocol is acceptable
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(initialSize = 100, size = 1000)
public class ProposalNotificationResponse implements Event, Poolable
{
    private ProposalNotificationEvent myProposalNotification;

    private boolean myCanAccept;

    /**
     * Fluent interface method for initializing the pooled object
     */
    public static ProposalNotificationResponse create(
        ProposalNotificationEvent proposalNotification,
        boolean canAccept)
    {
        ProposalNotificationResponse pnr =
            PooledObjects.getInstance(ProposalNotificationResponse.class);
        pnr.myProposalNotification = proposalNotification;
        pnr.myCanAccept            = canAccept;
        return pnr;
    }

    /**
     * Returns the value of canAccept
     */
    public boolean canAccept()
    {
        return myCanAccept;
    }

    /**
     * Returns the value of proposalNotification
     */
    public ProposalNotificationEvent getProposalNotification()
    {
        return myProposalNotification;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
        myProposalNotification = null;
        myCanAccept = false;
    }
}
