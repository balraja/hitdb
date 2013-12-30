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

package org.hit.communicator.test;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;

import org.hit.communicator.MessageSerializer;
import org.hit.communicator.NodeID;
import org.hit.communicator.ObjectStreamSerializer;
import org.hit.communicator.nio.IPNodeID;
import org.junit.Test;

/**
 * Defines the testcase for testing {@link ObjectStreamSerializer}
 * 
 * @author Balraja Subbiah
 */
public class ObjectStreamSerializerTest
{
    @Test
    public void test()
    {
        NodeID nodeID = new IPNodeID(10000);
        TestMessage message = new TestMessage(nodeID, 1001);
        MessageSerializer serializer = new ObjectStreamSerializer();
        ByteBuffer buffer = serializer.serialize(message);
        TestMessage deserializedMessage =
            (TestMessage) serializer.parse(buffer).iterator().next();
        
        assertNotNull(deserializedMessage);
        assertEquals(nodeID, deserializedMessage.getSenderId());
        assertEquals(1001, deserializedMessage.getValue());
    }
}
