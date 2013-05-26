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

package org.hit.distribution.test;

import java.util.Collection;
import junit.framework.TestCase;

import org.hit.communicator.NodeID;
import org.hit.partitioner.HashPartitioner;
import org.hit.partitioner.LinearPartitioner;
import org.hit.partitioner.Partitioner;
import org.hit.partitioner.domain.LongKeySpace;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * The test case for {@link HashPartitioner}.
 * 
 * @author Balraja Subbiah
 */
public class DistributedHashTableTest extends TestCase
{
    private static final NodeID FIRST_NODE = new StringNodeID("first");
    
    private static final NodeID SECOND_NODE = new StringNodeID("second");
    
    private static final NodeID THIRD_NODE = new StringNodeID("third");
    
    @Test
    public void testDHT()
    {
        Collection<NodeID> testNodes =
            Lists.newArrayList(FIRST_NODE, SECOND_NODE, THIRD_NODE);
        
        Partitioner<Long> longHashTable =
            new LinearPartitioner<>(new LongKeySpace(1L, 11L));
            
        longHashTable.distribute(testNodes);
        
        NodeID nodeHandlingValue = longHashTable.getNode(Long.valueOf(5));
        assertEquals(SECOND_NODE, nodeHandlingValue);
        
        nodeHandlingValue = longHashTable.getNode(Long.valueOf(10));
        assertEquals(THIRD_NODE, nodeHandlingValue);
    }

}
