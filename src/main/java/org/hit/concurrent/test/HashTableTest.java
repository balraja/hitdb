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

package org.hit.concurrent.test;

import java.util.List;

import org.hit.concurrent.HashTable;
import org.hit.concurrent.RefinableHashTable;

import com.google.common.collect.Lists;

/**
 * The test case for verifying the correctness of
 * <code>RefinableHashTable</code>
 * 
 * @author Balraja Subbiah
 */
public class HashTableTest extends
    AbstractConcurrentTest<HashTable<Integer, Integer>>
{
    private static final List<Object> CONST_RESULT1 =
        Lists.<Object>newArrayList(Boolean.TRUE,
                                   Boolean.TRUE,
                                   Integer.valueOf(3),
                                   Integer.valueOf(4));
    
    private static final List<Object> CONST_RESULT2 =
        Lists.<Object>newArrayList(Boolean.TRUE,
                                   Boolean.TRUE,
                                   Integer.valueOf(1),
                                   Integer.valueOf(2));

    public static void main(String[] args)
    {
        HashTableTest test = new HashTableTest();
        test.test();
    }

    /**
     * CTOR
     */
    public HashTableTest()
    {
        super(CONST_RESULT1,
              CONST_RESULT2,
              new RefinableHashTable<Integer, Integer>());;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TestCallable makeCallable1(HashTable<Integer, Integer> ds)
    {
         return  new TestCallable(ds) {
            
            /**
             * {@inheritDoc}
             */
            @Override
            public void doTest()
            {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                }
                
                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(1),
                                             Integer.valueOf(1))));
                
                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(2),
                                             Integer.valueOf(2))));
                
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                }
                
                register(
                    getTestedStructure().get(Integer.valueOf(3))
                                        .get(0));
                    
                register(
                     getTestedStructure().get(Integer.valueOf(4))
                                         .get(0));
            }
        };
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected TestCallable makeCallable2(HashTable<Integer, Integer> ds)
    {
        return  new TestCallable(ds) {
            
            /**
             * {@inheritDoc}
             */
            @Override
            public void doTest()
            {
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                }
                
                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(1),
                                             Integer.valueOf(1))));
                
                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(2),
                                             Integer.valueOf(2))));
                
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                }
                
                System.out.println(getTestedStructure().get(Integer.valueOf(3)));
                
                register(
                    getTestedStructure().get(Integer.valueOf(3))
                                        .get(0));
                    
                register(
                     getTestedStructure().get(Integer.valueOf(4))
                                         .get(0));
            }
        };
    }
}
