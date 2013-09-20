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

package org.hit.db.transactions;

import org.hit.db.model.Persistable;

/**
 * Defines the contract for an object that has necessary fields to be used
 * during transaction for validating the availability.
 * 
 * @author Balraja Subbiah
 */
public class Transactable<K extends Comparable<K>, P extends Persistable<K>>
{
    private long myStart;
    
    private long myEnd;
    
    private final P myPersitable;
    
    /**
     * CTOR
     */
    public Transactable(P persitable)
    {
        myPersitable = persitable;
    }

    /** Returns the time upto which this version is active */
    public long getEnd()
    {
        return myEnd;
    }

    /**
     * Returns the underlying <code>Persistable</code> that's protected by
     * this wrapper.
     */
    public P getPersistable()
    {
        return myPersitable;
    }
    
    /** Returns the time from which this version is active */
    public long getStart()
    {
        return myStart;
    }

    /**
     * Returns true if the object is valid at the given time for the
     * given transaction id.
     */
    public ValidationResult 
        validate(long time, long transactionID)
    {
        if (   !TransactionHelper.isTransactionID(myStart)
            && !TransactionHelper.isTransactionID(myEnd))
            
        {
            return new ValidationResult(myStart <= time  && time <= myEnd);
        }
        else {
            // if this is an old version that's updated by this transaction
            // then its valid.
            if (   !TransactionHelper.isTransactionID(myStart)
                && myStart < time
                && TransactionHelper.toTransactionID(myEnd) == transactionID)
            {
                return new ValidationResult(true);
            }
            // If an older version is locked by 
            else if (   !TransactionHelper.isTransactionID(myStart)
                     && myStart < time
                     && TransactionHelper.toTransactionID(myEnd) < transactionID
                     && (Registry.getState(TransactionHelper.toTransactionID(myEnd))
                              .in(TransactionState.ABORTED, 
                                  TransactionState.VALIDATE,
                                  TransactionState.COMMITTED)))
            {
                return new ValidationResult(
                    TransactionHelper.toTransactionID(myEnd));
            }
            // If this is the version created by this transaction then its valid.
            else if (   TransactionHelper.toTransactionID(myStart)
                            == transactionID
                     && !TransactionHelper.isTransactionID(myEnd)
                     && myEnd >= time)
            {
                return new ValidationResult(true);
            }
            // If this is the new version created by a a preceding transaction
            // then still allow the values to be read by this transaction.
            // speculatively. By speculative we add an entry to dependency 
            // graph.
            else if (   TransactionHelper.toTransactionID(myStart)
                            < transactionID
                     && Registry.getState(TransactionHelper.toTransactionID(
                            myStart)).in(
                                TransactionState.VALIDATE,
                                TransactionState.COMMITTED)
                     && !TransactionHelper.isTransactionID(myEnd)
                     && myEnd >= time)
            {
                return new ValidationResult(
                    TransactionHelper.toTransactionID(myStart));
            }
            else {
                return new ValidationResult(false);
            }
        }
    }

    /** Sets the end time for this version */
    public void setEnd(long end)
    {
        myEnd = end;
    }
    
    /** Sets the start time for this version */
    public void setStart(long start)
    {
        myStart = start;
    }
}
