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
package org.hit.buffer;

import org.hit.util.ApplicationProperties;

/**
 * Defines an implementation of {@link BufferConfig} where the values for the 
 * given namespace is read from the properties.
 * 
 * @author Balraja Subbiah
 */
public class BufferPropertyConfig implements BufferConfig
{
    private static final String NS_PREFIX = "org.hit.buffer.size";

    /**
     * {@inheritDoc}
     */
    @Override
    public int getBufferSize(String namespace)
    {
        String full_ns = NS_PREFIX + '.' + namespace;
        String buf_size = ApplicationProperties.getProperty(full_ns);
        return buf_size != null ? Integer.parseInt(buf_size) : -1;
    }
}
