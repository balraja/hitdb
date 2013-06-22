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
import java.math.BigInteger;
import java.util.Arrays;

import com.google.common.base.Preconditions;

/**
 * This class defines the range of numbers that can be generated with an
 * arbitrary number of bits.
 * 
 * @author Balraja Subbiah
 */
public class BigIntegerDomain implements DiscreteDomain<BigInteger>
{
    private BigInteger myMinimum;
    
    private BigInteger myMaximum;

    /**
     * CTOR
     */
    public BigIntegerDomain(int bits)
    {
        super();
        Preconditions.checkArgument(bits % 8 == 0, 
                                    "The given number of bits %s doesn't "
                                    + " match byte size limit",
                                    bits);
        byte[] numberAsBytes = new byte[bits / 8];
        byte zero = 0x00;
        Arrays.fill(numberAsBytes, zero);
        byte max = -128;
        numberAsBytes[0] = max;
        myMinimum = new BigInteger(numberAsBytes.clone());
        
        for (int i = 0; i < numberAsBytes.length; i++) {
            if (i == 0) {
                numberAsBytes[i] = 127;
            }
            else {
                numberAsBytes[i] = -1;
            }
        }
        myMaximum = new BigInteger(numberAsBytes);
    }

    /**
     * Returns the value of minimum
     */
    public BigInteger getMinimum()
    {
        return myMinimum;
    }

    /**
     * Returns the value of maximum
     */
    public BigInteger getMaximum()
    {
        return myMaximum;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myMinimum);
        out.writeObject(myMaximum);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException,
            ClassNotFoundException
    {
        myMinimum = (BigInteger) in.readObject();
        myMaximum = (BigInteger) in.readObject();
        
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BigInteger elementAt(long index)
    {
        return myMinimum.add(BigInteger.valueOf(index));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long getTotalElements()
    {
        return myMaximum.subtract(myMinimum).longValue();
    }
}
