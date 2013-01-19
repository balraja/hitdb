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

package org.hit.actors;

/**
 * The identifier for an actor. We abstract this as a type because it will
 * give us flexibility to change the identification mechanism if required later.
 * 
 * @author Balraja Subbiah
 */
public class ActorID
{
    private final String myIdentifier;

    /**
     * CTOR
     */
    public ActorID(String identifier)
    {
        myIdentifier = identifier;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ActorID other = (ActorID) obj;
        if (myIdentifier == null) {
            if (other.myIdentifier != null) {
                return false;
            }
        }
        else if (!myIdentifier.equals(other.myIdentifier)) {
            return false;
        }
        return true;
    }

    /**
     * Returns the value of identifier
     */
    public String getIdentifier()
    {
        return myIdentifier;
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
                 + ((myIdentifier == null) ? 0 : myIdentifier.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "ActorID [myIdentifier=" + myIdentifier + "]";
    }
}
