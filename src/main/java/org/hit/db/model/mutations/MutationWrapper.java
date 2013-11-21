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
package org.hit.db.model.mutations;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.hit.db.model.Mutation;

/**
 * A simple class to create {@link Mutation}s based on the data.
 * 
 * @author Balraja Subbiah
 */
public class MutationWrapper
{
    private static final String METHOD_NAME = "getMutationFactory";
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static Mutation wrapRangeMutation(
        Class<?>  dataClass,
        List<? extends Object>   data)
    {
        try {
            Method method = dataClass.getMethod(METHOD_NAME);
            MutationFactory factory = (MutationFactory) method.invoke(null);
            return factory.makeBatchAddMutation(data);
        }
        catch (NoSuchMethodException 
              | SecurityException
              | IllegalAccessException 
              | IllegalArgumentException
              | InvocationTargetException e) 
        {
            return null;
        }
    }
}
