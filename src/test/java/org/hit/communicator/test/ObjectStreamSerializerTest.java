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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;

import org.hit.communicator.BinaryMessage;
import org.hit.communicator.MessageSerializer;
import org.hit.communicator.NodeID;
import org.hit.communicator.ObjectStreamSerializer;
import org.hit.communicator.nio.IPNodeID;
import org.hit.io.buffer.BufferManager;
import org.hit.io.buffer.ManagedBuffer;
import org.hit.io.pool.PoolableIOFactory;
import org.hit.pool.SimplePoolableRegistry;
import org.junit.Assert;
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
        MessageSerializer serializer = 
            new ObjectStreamSerializer(new BufferManager(20), 
                                       new PoolableIOFactory(
                                           new SimplePoolableRegistry()));
        BinaryMessage binaryMessage = serializer.serialize(message);
        TestMessage deserializedMessage =
            (TestMessage) serializer.parse(binaryMessage).iterator().next();
        
        assertNotNull(deserializedMessage);
        assertEquals(nodeID, deserializedMessage.getSenderId());
        assertEquals(1001, deserializedMessage.getValue());
    }
    
    public void testMessageWithSerializables()
    {
        NodeID nodeID = new IPNodeID(10000);
        Map<String,String> testData = new HashMap<>();
        testData.put("hello", "world");
        MessageWithSerializable mws = 
            new MessageWithSerializable(nodeID, testData);
        MessageSerializer serializer = 
                new ObjectStreamSerializer(new BufferManager(20), 
                        new PoolableIOFactory(
                            new SimplePoolableRegistry()));
        BinaryMessage binaryMessage = serializer.serialize(mws);
        MessageWithSerializable deserializedMessage =
            (MessageWithSerializable) 
                serializer.parse(binaryMessage).iterator().next();
        
        assertNotNull(deserializedMessage);
        assertEquals(nodeID, deserializedMessage.getSenderId());
        Assert.assertEquals(deserializedMessage.getTestData(), 
                            testData);

    }
    
    @Test
    public void testBigMessage()
    {
        NodeID nodeID = new IPNodeID(10000);
        BigTestMessage message = new BigTestMessage(nodeID);
        MessageSerializer serializer = 
                new ObjectStreamSerializer(new BufferManager(20), 
                        new PoolableIOFactory(
                            new SimplePoolableRegistry()));
        BinaryMessage binaryMessage = serializer.serialize(message);
        BigTestMessage deserializedMessage =
            (BigTestMessage) serializer.parse(binaryMessage).iterator().next();
        
        assertNotNull(deserializedMessage);
        assertEquals(nodeID, deserializedMessage.getSenderId());
        Assert.assertArrayEquals(message.getTestData(), 
                                 deserializedMessage.getTestData());
    }
    
    @Test
    public void testChannelWrite() throws IOException
    {
        File tempFile = File.createTempFile("message", "");
        RandomAccessFile raf = new RandomAccessFile(tempFile, "rw");
        FileChannel channel = raf.getChannel();
        NodeID nodeID = new IPNodeID(10000);
        BigTestMessage message = new BigTestMessage(nodeID);
        BufferManager manager = new BufferManager(5);
        
        MessageSerializer serializer = 
                new ObjectStreamSerializer(new BufferManager(20), 
                        new PoolableIOFactory(
                            new SimplePoolableRegistry()));
        BinaryMessage binaryMessage = serializer.serialize(message);
        binaryMessage.writeTo(channel);
        
        channel.force(true);
        
        channel.position(0);
        BinaryMessage readBinaryMessage = new ManagedBuffer(manager);
        readBinaryMessage.readFrom(channel);
        BigTestMessage deserializedMessage =
            (BigTestMessage) serializer.parse(readBinaryMessage).iterator().next();
        assertNotNull(deserializedMessage);
        assertEquals(nodeID, deserializedMessage.getSenderId());
        Assert.assertArrayEquals(message.getTestData(), 
                                 deserializedMessage.getTestData());
        
        raf.close();
        tempFile.delete();
    }
}
