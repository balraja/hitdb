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

package org.hit.util;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Extends <code>ThreadGroup</code> to support handling uncaught exceptions.
 * 
 * @author Balraja Subbiah
 */
public class TerminatingThreadGroup extends ThreadGroup
{
    private static final Logger LOG =
        LogFactory.getInstance().getLogger(TerminatingThreadGroup.class);
    
    /**
     * CTOR
     */
    public TerminatingThreadGroup(String name)
    {
        super(name);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void uncaughtException(Thread t, Throwable e)
    {
        LOG.severe("Received exception in thread " + t.getName());
        LOG.log(Level.SEVERE, e.getMessage(), e);
        super.uncaughtException(t, e);
    }
}
