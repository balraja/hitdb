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
package org.hit.io.pool;

import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.hit.io.ObjectIOFactory;

import com.google.inject.Inject;

/**
 * Implements <code>ObjectIOFactory</code> to support using poolable 
 * instances when deserializng the data.
 * 
 * @author Balraja Subbiah
 */
public class PoolableIOFactory implements ObjectIOFactory
{
    private final PoolableRegistry myRegistry;

    /**
     * CTOR
     */
    @Inject
    public PoolableIOFactory(PoolableRegistry registry)
    {
        super();
        myRegistry = registry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectOutput getOutput(OutputStream out)
    {
        return new PoolableOutput(out, myRegistry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectInput getInput(InputStream in)
    {
        return new PoolableInput(in, myRegistry);
    }
}
