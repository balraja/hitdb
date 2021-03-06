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
package org.hit.communicator.nio;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.ref.WeakReference;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.pool.Interner;
import org.hit.util.LogFactory;

/**
 * Extends <code>Interner</code> to support reading writing <code>IPNodeID
 * </code> values out of a network.
 * 
 * @author Balraja Subbiah
 */
public class IPNodeIDInterner extends Interner<IPNodeID>
{
    private static final Logger LOG =
            LogFactory.getInstance().getLogger(IPNodeIDInterner.class);
                    
    private final Map<String, TIntObjectMap<WeakReference<IPNodeID>>>
         myIdentifierCache = new HashMap<>();
         
    /**
     * CTOR
     */
    public IPNodeIDInterner()
    {
    }
    
    private IPNodeID doConstructInstance(String hostAddress, int port) 
            throws IOException
    {
        TIntObjectMap<WeakReference<IPNodeID>> hostMap = 
            myIdentifierCache.get(hostAddress);
        
        if (hostMap == null) {
            hostMap= new TIntObjectHashMap<>();
            myIdentifierCache.put(hostAddress.intern(), hostMap);
        }
        
        WeakReference<IPNodeID> reference = hostMap.get(port);
        IPNodeID nodeID = reference != null ? reference.get() : null;
        if (nodeID == null) {
            try {
                nodeID =
                    new IPNodeID(new InetSocketAddress(
                            InetAddress.getByName(hostAddress),
                            port));
            }
            catch (UnknownHostException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
                throw new IOException(e);
            }
            
           reference = new WeakReference<IPNodeID>(nodeID);
           hostMap.put(port, reference);
        }
        return nodeID;
    }

    /**
     * {@inheritDoc}
     * @throws IOException 
     */
    @Override
    public IPNodeID readFromInput(ObjectInput input) throws IOException
    {
        String hostAddress = input.readUTF().intern();
        int    port        = input.readInt();
        return doConstructInstance(hostAddress, port);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeToOutput(ObjectOutput output, IPNodeID instance) 
        throws IOException
    {
        output.writeUTF(instance.getIPAddress().getHostString());
        output.writeInt(instance.getIPAddress().getPort());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IPNodeID contructInstance(Object... parameters)
    {
        try {
            return doConstructInstance((String)  parameters[0],
                                       (Integer) parameters[1]);
        }
        catch (IOException e) {
            return null;
        }
    }
}
