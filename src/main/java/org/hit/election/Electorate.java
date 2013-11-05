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
package org.hit.election;

import java.util.HashSet;
import java.util.Set;

import org.hit.communicator.NodeID;
import org.hit.zookeeper.ZooKeeperClient;

/**
 * Defines the set nodes that participates in a election.
 * 
 * @author Balraja Subbiah
 */
public class Electorate
{
    private final ElectorateID myID;
    
    private final Set<NodeID> myNodes;
    
    private final long myTerm;
    
    private final ZooKeeperClient myZKClient;

    /**
     * CTOR
     */
    public Electorate(ElectorateID id, ZooKeeperClient zc)
    {
        myID    = id;
        myNodes = new HashSet<>();
        myTerm  = 0L;
        myZKClient = zc;
    }
    
    /**
     * CTOR
     */
    public Electorate(ElectorateID    id, 
                      ZooKeeperClient zc, 
                      NodeID          node, 
                      long            term)
    {
        myID    = id;
        myNodes = new HashSet<>();
        myNodes.add(node);
        myTerm = term;
        myZKClient = zc;
    }
    
    /**
     * Adds a node to the electorate
     */
    public void add(NodeID node)
    {
        myNodes.add(node);
    }

    /**
     * Returns the value of iD
     */
    public ElectorateID getID()
    {
        return myID;
    }

    /**
     * Returns the value of nodes
     */
    public Set<NodeID> getNodes()
    {
        return myNodes;
    }

    /**
     * Returns the value of term
     */
    public long getTerm()
    {
        return myTerm;
    }
}
