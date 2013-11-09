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

import java.util.Set;

import org.hit.communicator.NodeID;
import org.hit.gms.GroupID;

/**
 * An {@link Event} to notify that a group is ready. 
 * 
 * @author Balraja Subbiah
 */
public class GroupReadyEvent implements Event
{
    private final GroupID myGroupID;
    
    private final long myTerm;
    
    private final NodeID myLeader;
    
    private final Set<NodeID> myFollowers;

    /**
     * CTOR
     */
    public GroupReadyEvent(
        GroupID groupID,
        long term,
        NodeID leader,
        Set<NodeID> followers)
    {
        super();
        myGroupID = groupID;
        myTerm = term;
        myLeader = leader;
        myFollowers = followers;
    }

    /**
     * Returns the value of groupID
     */
    public GroupID getGroupID()
    {
        return myGroupID;
    }

    /**
     * Returns the value of term
     */
    public long getTerm()
    {
        return myTerm;
    }

    /**
     * Returns the value of leader
     */
    public NodeID getLeader()
    {
        return myLeader;
    }

    /**
     * Returns the value of followers
     */
    public Set<NodeID> getFollowers()
    {
        return myFollowers;
    }
}
