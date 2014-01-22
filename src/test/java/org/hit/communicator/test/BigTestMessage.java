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
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Random;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;

/**
 * An example {@link Message} which stores large amount of data.
 * 
 * @author Balraja Subbiah
 */
public class BigTestMessage extends Message
{
    private int[] myTestData;
    
    /**
     * CTOR
     */
    public BigTestMessage()
    {
        super();
    }

    /**
     * CTOR
     */
    public BigTestMessage(NodeID senderId)
    {
        super(senderId);
        myTestData = new int[1000];
        Random random = new Random(1);
        for (int i = 0; i < myTestData.length; i++) {
            myTestData[i] = random.nextInt();
        }
    }
    
    /**
     * Returns the value of testData
     */
    public int[] getTestData()
    {
        return myTestData;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException
    {
        super.readExternal(in);
        int size = in.readInt();
        myTestData = new int[size];
        for (int i = 0; i < size; i++) {
            myTestData[i] = in.readInt();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeInt(myTestData.length);
        for (int i = 0; i < myTestData.length; i++) {
            out.writeInt(myTestData[i]);
        }
    }
}
