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
package org.hit.di;

import com.google.inject.Provider;
import com.google.inject.name.Named;

import org.hit.communicator.NodeID;
import org.hit.server.ServerConfig;
import org.hit.server.ServerID;

/**
 * Extends {@link Provider} to generate server id from the port no 
 * and the server name.
 * 
 * @author Balraja Subbiah
 */
public class ServerIDProvider implements Provider<NodeID>
{
    private final NodeID myServerID;

    /**
     * CTOR
     */
    public ServerIDProvider(@Named("PreferredPort") Integer port,
                            ServerConfig            config)
    {
        myServerID = new ServerID(port, config.getServerName());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public NodeID get()
    {
        return myServerID;
    }

}
