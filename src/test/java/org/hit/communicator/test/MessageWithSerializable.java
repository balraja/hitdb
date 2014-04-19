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
import java.util.HashMap;
import java.util.Map;

import org.hit.communicator.Message;
import org.hit.communicator.NodeID;

/**
 * A test {@link Message} that contains a {@link Serializable}.
 * 
 * @author Balraja Subbiah
 */
public class MessageWithSerializable extends Message
{
    private Map<String, String> myTestData;
    
    /**
     * CTOR
     */
    public MessageWithSerializable()
    {
        myTestData = new HashMap<>();
    }

    /**
     * CTOR
     */
    public MessageWithSerializable(NodeID senderId,
                                   Map<String,String> hashMap)
    {
        super(senderId);
        myTestData = hashMap;
    }
    
    /**
     * Returns the value of testData
     */
    public Map<String, String> getTestData()
    {
        return myTestData;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) 
        throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        myTestData = (HashMap<String, String>) in.readObject();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeObject(myTestData);
    }
}
