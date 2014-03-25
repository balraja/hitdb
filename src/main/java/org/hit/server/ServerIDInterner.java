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
package org.hit.server;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.WeakHashMap;

import org.hit.pool.Interner;

/**
 * Implements {@link Interner} for {@link ServerNodeID} type.
 * 
 * @author Balraja Subbiah
 */
public class ServerIDInterner extends Interner<ServerNodeID>
{
    private final Map<String, ServerNodeID> myNameToIDCache;
    
    /**
     * CTOR
     */
    public ServerIDInterner()
    {
        myNameToIDCache = new WeakHashMap<>();
    }
    
    private ServerNodeID doConstructInstance(String name, String host, int port)
    {
        ServerNodeID serverID = myNameToIDCache.get(name);
        if (serverID != null 
            && serverID.getIPAddress().getHostString().equals(host)
            && serverID.getIPAddress().getPort() == port)
        {
            return serverID;
        }
        else {
            serverID = 
                new ServerNodeID(InetSocketAddress.createUnresolved(host, port), 
                                 name);
            myNameToIDCache.put(name, serverID);
            return serverID;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ServerNodeID readFromInput(ObjectInput input) throws IOException
    {
        String name = input.readUTF().intern();
        String host = input.readUTF().intern();
        int    port = input.readInt();
        return doConstructInstance(name, host, port);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeToOutput(ObjectOutput output, ServerNodeID instance)
            throws IOException
    {
        output.writeUTF(instance.getName());
        output.writeUTF(instance.getIPAddress().getHostString());
        output.writeInt(instance.getIPAddress().getPort());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ServerNodeID contructInstance(Object... params)
    {
        return doConstructInstance((String)  params[0],
                                   (String)  params[1],
                                   (Integer) params[2]);
    }
}
