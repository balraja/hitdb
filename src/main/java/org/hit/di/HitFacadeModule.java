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

package org.hit.di;

import com.google.inject.Provides;
import com.google.inject.name.Named;

import org.hit.communicator.NodeID;
import org.hit.communicator.nio.IPNodeID;
import org.hit.registry.RegistryService;
import org.hit.registry.ZKRegistry;

/**
 * Extends <code>HitModule</code> to support adding bindings for the
 * client side.
 * 
 * @author Balraja Subbiah
 */
public class HitFacadeModule extends HitModule
{
    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure()
    {
        super.configure();
        bind(RegistryService.class).to(ZKRegistry.class);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getDefaultBoundPort()
    {
        return Integer.valueOf(16000);
    }
    
    @Provides
    protected NodeID provideNodeID(@Named("PreferredPort") Integer port)
    {
        return new IPNodeID(port);
    }
}
