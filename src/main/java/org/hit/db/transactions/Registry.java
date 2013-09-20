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

package org.hit.db.transactions;

import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hit.concurrent.CloseableRWLock;

/**
 * Defines the contract for a type that defines context of transactions.
 * 
 * @author Balraja Subbiah
 */
public final class Registry
{
    private static CloseableRWLock ourLock = 
         new CloseableRWLock(new ReentrantReadWriteLock(true));
    
    private static TLongObjectMap<TransactionState> ourStateMap = 
        new TLongObjectHashMap<>();
    
    private static TLongObjectHashMap<TLongSet> ourDependenetTransactions =
        new TLongObjectHashMap<>();
        
    private static TLongObjectHashMap<TLongSet> ourDependingTransactions =
        new TLongObjectHashMap<>();
    
    /**
     * Returns the current {@link TransactionState} for the given id.
     */
    public static TransactionState getState(long id)
    {
        TransactionState result = TransactionState.UNKNOWN;
        try (CloseableRWLock l = ourLock.openReadLock()) {
            TransactionState existing = ourStateMap.get(id);
            if (existing != null) {
                result = existing;
            }
            return result;
        }
    }
    
    /**
     * Returns the current {@link TransactionState} for the given id.
     */
    public static void updateTransactionState(long id, TransactionState state)
    {
        try (CloseableRWLock l = ourLock.openWriteLock()) {
            ourStateMap.put(id, state);
        }
    }
    
    /**
     * A helper method to add dependency between two transactions.
     */
    public static void addDependency(long from, long to)
    {
        try (CloseableRWLock l = ourLock.openWriteLock()) {
            TLongSet dependentTransactions = 
                ourDependenetTransactions.get(from);
            if (dependentTransactions == null) {
                dependentTransactions = new TLongHashSet();
                ourDependenetTransactions.putIfAbsent(from, 
                                                      dependentTransactions);
            }
            dependentTransactions.add(to);
            
            TLongSet dependeningTransactions = 
                ourDependingTransactions.get(from);
            if (dependeningTransactions == null) {
                dependeningTransactions = new TLongHashSet();
                ourDependingTransactions.putIfAbsent(to, 
                                                     dependeningTransactions);
            }
            dependeningTransactions.add(from);
        }
    }
    
    /**
     * A helper method to update dependency graphs when a transaction is 
     * done. 
     */
    public static TLongSet freeDependentTransactionsOn(long from)
    {
        TLongSet result = new TLongHashSet();
        try (CloseableRWLock l = ourLock.openWriteLock()) {
            ourDependingTransactions.remove(from);
            
            TLongSet dependentTransactions = 
                ourDependenetTransactions.remove(from);
            
            if (dependentTransactions != null) {
                for (long dependentTrn : dependentTransactions.toArray()) {
                    TLongSet dependeningTransactions = 
                        ourDependingTransactions.get(from);
                    if (dependeningTransactions != null) {
                        dependeningTransactions.remove(from);
                        if (dependeningTransactions.isEmpty()) {
                            result.add(dependentTrn);
                        }
                    }
                }
            }
            
            return result;
        }
    }
    
    /** 
     * Returns the list of trnsactions on which a given transaction 
     * is dependent upon.
     */
    public static TLongSet getDependencyOn(long transactionID)
    {
        try (CloseableRWLock l = ourLock.openWriteLock()) {
            return ourDependingTransactions.get(transactionID);
        }
    }
}
