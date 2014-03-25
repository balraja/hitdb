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
package org.hit.pool;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.hit.util.LogFactory;

/**
 * Defines the contract for a type that's responsible for serializing,
 * deserializing internable objects in the system.
 * 
 * @author Balraja Subbiah
 */
public abstract class Interner<T extends Internable>
{
    private static final Logger LOG = 
        LogFactory.getInstance().getLogger(Interner.class);
    
    private static final Map<Class<?>, Interner<?>> ourTypeToInternMap = 
        new HashMap<Class<?>, Interner<?>>();
    
    /**
     * Returns the {@link Interner} for the given type.
     */
    public static Interner<?> getInterner(Class<?> internableType)
    {
        Interner<?> interner = ourTypeToInternMap.get(internableType);
        if (interner == null) {
            InternedBy internedBy = 
                internableType.getAnnotation(InternedBy.class);
            try {
                interner = internedBy.interner().newInstance();
                ourTypeToInternMap.put(internableType, interner);
            }
            catch (InstantiationException | IllegalAccessException e) {
                LOG.log(Level.SEVERE, e.getMessage(), e);
            }
        }
        return interner;
    }
    
    /**
     * CTOR
     */
    public Interner()
    {
    }
    
    /**
     * Takes an instance of object and casts it to {@link Internable}.
     */
    @SuppressWarnings("unchecked")
    public void writeToOutput(ObjectOutput output, Object instance) 
        throws IOException
    {
        writeToOutput(output, (T) instance); 
    }
    
    /**
     * Reads the value of {@link Internable} from the {@link ObjectInput}
     */
    public abstract T readFromInput(ObjectInput input) throws IOException;
    
    /**
     * Writes the value of {@link Internable} to the {@link ObjectOutput}
     */
    public abstract void writeToOutput(ObjectOutput output, T instance) 
        throws IOException;
    
    /**
     * Returns the value of instance from the parameters.
     */
    public abstract T contructInstance(Object...parameters);
}
