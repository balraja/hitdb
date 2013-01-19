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

package org.hit.concurrent.test;

import java.util.List;

import org.hit.concurrent.LocklessSortedList;

import com.google.common.collect.Lists;

/**
 * Tests the <code>LocklessSortedList<Integer>
 * 
 * @author Balraja Subbiah
 */
public class LocklessSortedListTest 
    extends AbstractConcurrentTest<LocklessSortedList<Integer>>
{
    private static final List<Object> CONST_RESULT1 = 
            Lists.<Object>newArrayList(Boolean.valueOf(true), 
                                       Boolean.valueOf(true),
                                       Boolean.valueOf(true),
                                       Boolean.valueOf(true));
    
    private static final List<Object> CONST_RESULT2 = 
            Lists.<Object>newArrayList(Boolean.valueOf(true), 
                    Boolean.valueOf(true),
                    Boolean.valueOf(true),
                    Boolean.valueOf(true));

    /**
     * CTOR
     */
    public LocklessSortedListTest()
    {
        super(CONST_RESULT1, CONST_RESULT2, new LocklessSortedList<Integer>());
    }

    @Override
    protected TestCallable makeCallable1(LocklessSortedList<Integer> ds)
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
                    getTestedStructure().add(Integer.valueOf(1))));
                
                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(2))));
                
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                }
                
                register(Boolean.valueOf(
                    getTestedStructure().contains(Integer.valueOf(3))));
                    
                register(Boolean.valueOf(
                    getTestedStructure().contains(Integer.valueOf(4))));
            }
        };
    }

    @Override
    protected TestCallable makeCallable2(LocklessSortedList<Integer> ds)
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
                    getTestedStructure().add(Integer.valueOf(3))));
                
                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(4))));
                
                try {
                    Thread.sleep(100);
                }
                catch (InterruptedException e) {
                    // ignore
                }
                
                register(Boolean.valueOf(
                    getTestedStructure().contains(Integer.valueOf(1))));
                    
                register(Boolean.valueOf(
                    getTestedStructure().contains(Integer.valueOf(2))));
            }
        };
    }
    
    public static void main(String[] args)
    {
        LocklessSortedListTest test = new LocklessSortedListTest();
        test.test();
    }
}
