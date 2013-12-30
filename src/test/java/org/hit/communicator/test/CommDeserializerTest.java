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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.hit.communicator.MessageSerializer;
import org.hit.communicator.NodeID;
import org.hit.communicator.ObjectStreamSerializer;
import org.hit.communicator.nio.IPNodeID;
import org.hit.db.model.mutations.BatchAddMutation;
import org.hit.example.Airport;
import org.hit.example.AirportDataLoader;
import org.hit.messages.DBOperationMessage;
import org.junit.Test;

/**
 * Defines a test case for the way by which deserialize the objects.
 * 
 * @author Balraja Subbiah
 */
public class CommDeserializerTest
{
    private static final int BUFFER_SIZE = 1024 * 10;
    
    @Test
    public void test()
    {
        NodeID nodeID = new IPNodeID(10000);
        List<Airport> airportList = new AirportDataLoader().loadTestData();
        BatchAddMutation<Long, Airport> batchMutation = 
            new BatchAddMutation<>("Airport", airportList);
        DBOperationMessage message = 
            new DBOperationMessage(nodeID, 1L, batchMutation);
        
        MessageSerializer serializer = new ObjectStreamSerializer();
        ByteBuffer buffer = serializer.serialize(message);
        ByteBuffer readData = simulateTcpRead(buffer);
        assertNotNull(readData);
        DBOperationMessage deserailizedMessage = 
            (DBOperationMessage) serializer.parse(readData).iterator().next();
        assertNotNull(deserailizedMessage);
        
    }
    
    private ByteBuffer simulateTcpRead(ByteBuffer inBuffer)
    {
        ByteArrayInputStream bin = new ByteArrayInputStream(inBuffer.array());
        byte[] buffer = new byte[BUFFER_SIZE];
        ByteBuffer messageBuffer = ByteBuffer.allocate(10 * 1024 * 1024);
        int readBytes = -1;
        try {
            while ((readBytes = bin.read(buffer)) != -1) {
                messageBuffer.put(buffer, 0, readBytes);
            }
            messageBuffer.flip();
            return messageBuffer;
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            try {
                bin.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
