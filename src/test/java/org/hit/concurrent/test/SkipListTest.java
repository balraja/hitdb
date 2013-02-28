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
import java.util.concurrent.CountDownLatch;

import org.hit.concurrent.LocklessSkipList;

import com.google.common.collect.Lists;

/**
 * The test case for verifying the correctness of concurrent skip list.
 *
 * @author Balraja Subbiah
 */
public class SkipListTest extends
    AbstractConcurrentTest<LocklessSkipList<Integer, Integer>>
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

    /**
     * CTOR
     */
    public SkipListTest()
    {
        super(CONST_RESULT1,
              CONST_RESULT2,
              new LocklessSkipList<Integer, Integer>(5));;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TestCallable makeCallable1(LocklessSkipList<Integer, Integer> ds,
                                         final CountDownLatch dataLoadPont)
    {
         return  new TestCallable(ds) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void doTest()
            {

                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(1),
                                             Integer.valueOf(1))));

                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(2),
                                             Integer.valueOf(2))));

                dataLoadPont.countDown();
                try {
                    dataLoadPont.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                register(
                    getTestedStructure().lookupValue(Integer.valueOf(3))
                                        .get(0));

                register(
                     getTestedStructure().lookupValue(Integer.valueOf(4))
                                         .get(0));
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected TestCallable makeCallable2(LocklessSkipList<Integer, Integer> ds,
                                         final CountDownLatch dataLoadPoint)
    {
        return  new TestCallable(ds) {

            /**
             * {@inheritDoc}
             */
            @Override
            public void doTest()
            {
                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(3),
                                             Integer.valueOf(3))));

                register(Boolean.valueOf(
                    getTestedStructure().add(Integer.valueOf(4),
                                             Integer.valueOf(4))));

                dataLoadPoint.countDown();
                try {
                    dataLoadPoint.await();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }

                register(
                    getTestedStructure().lookupValue(Integer.valueOf(1))
                                        .get(0));

                register(
                     getTestedStructure().lookupValue(Integer.valueOf(2))
                                         .get(0));
            }
        };
    }
}
