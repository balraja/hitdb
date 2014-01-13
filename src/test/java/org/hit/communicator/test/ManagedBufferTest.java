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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.hit.buffer.BufferManager;
import org.hit.buffer.ManagedBufferInputStream;
import org.hit.buffer.ManagedBufferOutputStream;
import org.hit.communicator.NodeID;
import org.hit.communicator.nio.IPNodeID;
import org.junit.Assert;
import org.junit.Test;

/**
 * An unit test for verifying the correctness of serialization/deserialization
 * using {@link ManagedBufferOutputStream} and  {@link ManagedBufferInputStream}
 * 
 * @author Balraja Subbiah
 */
public class ManagedBufferTest
{
    @Test
    public void testBufferSerialization()
    {
        NodeID nodeID = new IPNodeID(10000);
        TestMessage message = new TestMessage(nodeID, 1001);
        
        BufferManager manager = new BufferManager(20);
        try {
            ManagedBufferOutputStream mout = 
                new ManagedBufferOutputStream(manager);
            ObjectOutputStream objectOutput = new ObjectOutputStream(mout);
            objectOutput.writeObject(message);
            objectOutput.close();
            
            ManagedBufferInputStream min = 
                ManagedBufferInputStream.wrapSerializedData(mout.getWrittenData());
            ObjectInputStream objectInput = new ObjectInputStream(min);
            TestMessage readMessage = (TestMessage) objectInput.readObject();
            objectInput.close();
            
            Assert.assertEquals(message, readMessage);
        }
        catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }   
}
