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

package org.hit.key;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigInteger;

import org.hit.key.domain.BigIntegerDomain;
import org.hit.key.domain.DiscreteDomain;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * Defines the contract for a distributed hash table, wherein each node claims
 * the entries in a ring and services the data between previous node's entry
 * and current node's entry.
 *
 * @author Balraja Subbiah
 */
public class HashKeyspace<S extends Comparable<S>>
    implements Keyspace<S, BigInteger>
{
    /**
     * An enum to denote the various types of hash functions used in a
     * distributed partitioner.
     */
    public static enum HashFunctionID
    {
        GOOD_FAST_HASH(Hashing.goodFastHash(512));

        private final HashFunction myHashFunction;

        /**
         * CTOR
         */
        private HashFunctionID(HashFunction function)
        {
            myHashFunction = function;
        }

        /** Getter for the hash function */
        public HashFunction getHashFunction()
        {
            return myHashFunction;
        }
    }

    private Funnel<S> myFunnel;

    private HashFunctionID myHashFunctionID;

    private BigIntegerDomain myRange;

    /**
     * CTOR
     */
    public HashKeyspace()
    {
        myHashFunctionID = null;
        myFunnel = null;
        myRange = null;
    }

    /**
     * CTOR
     */
    public HashKeyspace(HashFunctionID functionID, Funnel<S> funnel)
    {
        myHashFunctionID = functionID;
        myFunnel = funnel;
        myRange =
            new BigIntegerDomain(functionID.getHashFunction().bits());
    }

    /**
     * Returns the value of funnel
     */
    public Funnel<S> getFunnel()
    {
        return myFunnel;
    }

    /**
     * Returns the value of hashFunctionID
     */
    public HashFunctionID getHashFunctionID()
    {
        return myHashFunctionID;
    }

    /** Returns the node corresponding to the given key */
    @Override
    public BigInteger map(S key)
    {
        BigInteger hashValue =
            new BigInteger(
                myHashFunctionID.getHashFunction()
                                .newHasher()
                                .putObject(key, myFunnel)
                                .hash()
                                .asBytes());
        return hashValue;

    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myHashFunctionID = HashFunctionID.valueOf(in.readUTF());
        myFunnel = (Funnel<S>) in.readObject();
        myRange =
            new BigIntegerDomain(myHashFunctionID.getHashFunction().bits());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myHashFunctionID.name());
        out.writeObject(myFunnel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DiscreteDomain<BigInteger> getDomain()
    {
        return myRange;
    }
}
