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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Defines the abstract testing infrastructure for concurrent data structures.
 * 
 * @author Balraja Subbiah
 */
public abstract class AbstractConcurrentTest<T>
{
    private final List<Object> myThread1ExpectedResult;
    
    private final List<Object> myThread2ExpectedResult;
    
    private final T myTestedStructure;
    
    private final ExecutorService myTestingExecutorService;
    
    /**
     * An abstract Callable for testing the given data structure.
     */
    public abstract class TestCallable implements Callable<List<Object>>
    {
        private final T myTestedStructure;
        
        private final List<Object> myActualResult;
        
        /** CTOR */
        public TestCallable(T testedStructure)
        {
            myTestedStructure = testedStructure;
            myActualResult = new ArrayList<>();
        }
        
        /**
         * Returns the value of testedStructure
         */
        public T getTestedStructure()
        {
            return myTestedStructure;
        }

        /** Registers the given result to the list of expected results */
        public void register(Object result)
        {
            myActualResult.add(result);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<Object> call() throws Exception
        {
            doTest();
            return myActualResult;
        }
        
        public abstract void doTest();
    }

    /**
     * CTOR
     */
    public AbstractConcurrentTest(
        List<Object> thread1ExpectedResult,
        List<Object> thread2ExpectedResult, 
        T testedStructure)
    {
        myThread1ExpectedResult = thread1ExpectedResult;
        myThread2ExpectedResult = thread2ExpectedResult;
        myTestedStructure = testedStructure;
        myTestingExecutorService = Executors.newFixedThreadPool(2);
    }
    
    /** 
     * Performs testing of concurrent data structures by invoking them on 
     * two different threads and comparing the registered results with the
     * expected results.
     */
    public void test()
    {
        TestCallable callable1 = makeCallable1(myTestedStructure);
        TestCallable callable2 = makeCallable2(myTestedStructure);
        Future<List<Object>> result1 = 
                myTestingExecutorService.submit(callable1);
        Future<List<Object>> result2 =
                myTestingExecutorService.submit(callable2);
        
        List<Object> actualResult1;
        List<Object> actualResult2;
        try {
            actualResult1 = result1.get();
            actualResult2 = result2.get();
            System.out.println(
                "The expected result from thread 1 " + 
                 myThread1ExpectedResult.toString()
                + " got " + actualResult1
                + " matches " + myThread1ExpectedResult.equals(actualResult1));
            System.out.println(
                "The expected result from thread 2 " + 
                myThread2ExpectedResult.toString()
                + " got " + actualResult2
                + " matches " + myThread2ExpectedResult.equals(actualResult2));
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
    }
    
    protected abstract TestCallable makeCallable1(T ds);
    
    protected abstract TestCallable makeCallable2(T ds);

}
