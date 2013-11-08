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

import org.hit.election.ElectorateID;

/**
 * An event to join the {@link Electorate}. 
 * 
 * @author Balraja Subbiah
 */
public class JoinElectorateEvent implements Event
{
    private final boolean myLeader;
    
    private final ElectorateID myElectorateID;

    /**
     * CTOR
     */
    public JoinElectorateEvent(boolean leader, ElectorateID electorateID)
    {
        super();
        myLeader = leader;
        myElectorateID = electorateID;
    }

    /**
     * Returns the value of leader
     */
    public boolean isLeader()
    {
        return myLeader;
    }

    /**
     * Returns the value of electorateID
     */
    public ElectorateID getElectorateID()
    {
        return myElectorateID;
    }
}
