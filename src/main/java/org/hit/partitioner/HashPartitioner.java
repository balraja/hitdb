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

package org.hit.partitioner;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import org.hit.communicator.NodeID;

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
public class HashPartitioner<T extends Comparable<T>>
    implements Partitioner<T>
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

    private TreeMap<BigInteger, NodeID> myClaimedPositions;

    private Funnel<T> myFunnel;

    private HashFunctionID myHashFunctionID;

    private ArbitraryNumberRange myRange;

    /**
     * CTOR
     */
    public HashPartitioner()
    {
        myHashFunctionID = null;
        myFunnel = null;
        myRange = null;
        myClaimedPositions = null;
    }

    /**
     * CTOR
     */
    public HashPartitioner(HashFunctionID functionID, Funnel<T> funnel)
    {
        myHashFunctionID = functionID;
        myFunnel = funnel;
        myRange =
            new ArbitraryNumberRange(functionID.getHashFunction().bits());
        myClaimedPositions = new TreeMap<>();
    }

    /** Distributes the nodes over a hash ring */
    @Override
    public void distribute(Collection<NodeID> nodes)
    {
        BigInteger offset =
            myRange.getSize().divide(BigInteger.valueOf(nodes.size()));
        BigInteger nodeValue = myRange.getMinimum().add(offset);
        for (NodeID node : nodes) {
            myClaimedPositions.put(nodeValue, node);
            nodeValue = nodeValue.add(offset);
        }
    }

    /**
     * Returns the value of claimedPositions
     */
    public Map<BigInteger, NodeID> getClaimedPositions()
    {
        return Collections.<BigInteger, NodeID>unmodifiableMap(
            myClaimedPositions);
    }

    /**
     * Returns the value of funnel
     */
    public Funnel<T> getFunnel()
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
    public NodeID getNode(T key)
    {
        BigInteger hashValue =
            new BigInteger(
                myHashFunctionID.getHashFunction()
                                .newHasher()
                                .putObject(key, myFunnel)
                                .hash()
                                .asBytes());

        NodeID holdingNode = myClaimedPositions.get(hashValue);
        if (holdingNode == null) {
            Map.Entry<BigInteger, NodeID> nextEntry =
                myClaimedPositions.ceilingEntry(hashValue);
            if (nextEntry != null) {
                holdingNode = nextEntry.getValue();
            }
            else {
               BigInteger maxValue =  myClaimedPositions.lastKey();
               if (maxValue.compareTo(hashValue) < 0) {
                   nextEntry =
                       myClaimedPositions.ceilingEntry(
                           myRange.getMinimum());

                   if (nextEntry != null) {
                       holdingNode = nextEntry.getValue();
                   }
               }
            }
        }
        return holdingNode;
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
        myFunnel = (Funnel<T>) in.readObject();
        myRange =
            new ArbitraryNumberRange(myHashFunctionID.getHashFunction().bits());
        myClaimedPositions = new TreeMap<>();

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
}
