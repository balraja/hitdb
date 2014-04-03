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
package org.hit.io;

import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * Defines the contract for an interface that's responsible for creating
 * ObjectInput and ObjectOutput for serializing and deserializing the 
 * objects.
 * 
 * @author Balraja Subbiah
 */
public interface ObjectIOFactory
{
    /**
     * Returns {@link ObjectOutput} that wraps the given {@link OutputStream}
     * which in turn can be used for serializing objects.
     */
    public ObjectOutput getOutput(OutputStream out);
    
    /**
     * Returns {@link ObjectInput} that wraps the given {@link InputStream}
     * which in turn can be used for reading objects from that stream.
     */
    public ObjectInput  getInput(InputStream in);
    
}
