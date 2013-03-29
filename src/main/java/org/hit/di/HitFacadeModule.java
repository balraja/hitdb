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

import org.hit.registry.RegistryService;
import org.hit.zookeeper.ZooKeeperClient;

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
        bind(RegistryService.class).to(ZooKeeperClient.class);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected Integer getBoundPort()
    {
        return Integer.valueOf(16000);
    }
}
