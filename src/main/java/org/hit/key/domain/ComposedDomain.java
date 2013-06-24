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

package org.hit.key.domain;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

/**
 * Defines the case which is a enumeration of cross product between elements
 * of multiple domains.
 *
 * @author Balraja Subbiah
 */
public class ComposedDomain<C extends Composed<C>>
    implements DiscreteDomain<C>
{
    private Class<C> myClass;

    private Constructor<C> myConstructor;

    private List<DiscreteDomain<?>> myDomains;

    private long[] myLowerIndexSize;

    private C myMax;

    private C myMin;

    private long myTotalElements;
    
    /**
     * CTOR
     */
    public ComposedDomain()
    {
        myDomains = null;
        myClass = null;
    }

    /**
     * CTOR
     */
    public ComposedDomain(List<DiscreteDomain<?>> domains,
                            Class<C> typeClass)
    {
        myDomains = domains;
        myClass = typeClass;
        initialize();
    }
    
    private void initialize()
    {
        List<Object> minValues = 
            Lists.newArrayListWithCapacity(myDomains.size());
        List<Object> maxValues = 
            Lists.newArrayListWithCapacity(myDomains.size());
        
        for (int i = 0; i < myDomains.size(); i++) {
            minValues.add(myDomains.get(i).getMinimum());
            maxValues.add(myDomains.get(i).getMaximum());
        }
        
        myLowerIndexSize = new long[myDomains.size()];
        long totalElements = 1;
        for (int index = myDomains.size() - 1; index >= 0; index--) {
          
            if (index == myDomains.size() - 1) {
                myLowerIndexSize[index] = 1;
            }
            else {
                myLowerIndexSize[index] = totalElements;
            }
            totalElements *= myDomains.get(index).getTotalElements();
        }
        myTotalElements = totalElements;
        myMin = makeInstance(minValues);
        myMax = makeInstance(maxValues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public C elementAt(long index)
    {
        List<Object> arguments = new ArrayList<>();
        for (int i = 0; i < myLowerIndexSize.length; i++) {
            long currIndex = index / myLowerIndexSize[0];
            arguments.add(myDomains.get(i).elementAt(currIndex));
            index = index % myLowerIndexSize[0];
        }
        return makeInstance(arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public C getMaximum()
    {
        return myMax;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public C getMinimum()
    {
        return myMin;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalElements()
    {
        return myTotalElements;
    }

    private C makeInstance(List<Object> arguments)
    {
        try {
            if (myConstructor == null) {
                Class<?>[] parameterClasses =
                    Lists.transform(arguments,
                                    new Function<Object, Class<?>>() {
                                        @Override
                                        public Class<?> apply(Object input)
                                        {
                                            return input.getClass();
                                        }

                                    }).toArray(new Class<?>[arguments.size()]);

                myConstructor = myClass.getConstructor(parameterClasses);
            }
            return myConstructor.newInstance(arguments.toArray());
        }
        catch (NoSuchMethodException
               | SecurityException
               | InstantiationException
               | IllegalAccessException
               | IllegalArgumentException
               | InvocationTargetException e)
        {
            throw new UnsupportedOperationException(
                "Unable to create an instance of " + myClass.getSimpleName()
                + " using parameters " + arguments,
                e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myClass.getName());
        out.writeObject(myDomains);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myDomains = (List<DiscreteDomain<?>>) in.readObject();
        myClass = (Class<C>) Class.forName(in.readUTF());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public C getMiddleOf(C lowerValue, C upperValue)
    {
        List<Object> arguments = new ArrayList<>();
        List<Object> lowerList = new ArrayList<>();
        List<Object> upperList = new ArrayList<>();
        
        for (int i = 0; i < myDomains.size(); i++) {
            arguments.add(myDomains.get(i)
                                   .getMiddleOf(lowerList.get(i), 
                                                upperList.get(i)));
        }
        return makeInstance(arguments);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public C getMiddleOf(Object lowerValue, Object upperValue)
    {
        // TODO Auto-generated method stub
        return null;
    }
}
