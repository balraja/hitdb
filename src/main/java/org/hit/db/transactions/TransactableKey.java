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

/**
 * Defines the contract for the key to be used with <code>Transactable</code>
 * objects.
 * 
 * @author Balraja Subbiah
 */
public class TransactableKey<K extends Comparable<K>>
    implements Comparable<TransactableKey<K>>
{
    private final K myPersitableKey;
    
    private long myStart;
    
    private long myEnd;

    /**
     * CTOR
     */
    public TransactableKey(K persitableKey)
    {
        super();
        myPersitableKey = persitableKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(TransactableKey<K> o)
    {
        int comparableResult = myPersitableKey.compareTo(o.getPersitableKey());
        // The underlying keys are equal, so compare the versions.
        if (comparableResult == 0) {
            // First case both objects are versioned accurately.
            if (       !TransactionHelper.isTransactionID(myStart)
                    && !TransactionHelper.isTransactionID(myEnd)
                    && !TransactionHelper.isTransactionID(o.getStart())
                    && !TransactionHelper.isTransactionID(o.getEnd()))
            {
                // If the start times are equal then compare the end time.
                if (myStart == o.getStart()) {
                    return (int) (myEnd - o.getEnd());
                }
                else {
                    // Order them by thier transaction identifiers.
                    return (int)
                           ( TransactionHelper.toTransactionID(myStart)
                             -
                             TransactionHelper.toTransactionID(o.getStart()));
                }
            }
            // Second case. Both the keys were updates created newly by the
            // transactions which are yet to be committed.
            else if (   TransactionHelper.isTransactionID(myStart)
                     && myEnd == TransactionHelper.INFINITY
                     && TransactionHelper.isTransactionID(o.getStart())
                     && o.getEnd() == TransactionHelper.INFINITY)
            {
                // If they belong to the same transaction ?, this should
                // never happen.
                if (myStart == o.getStart()) {
                    return 0;
                }
                else {
                    // Order them by thier transaction identifiers.
                    return (int)
                           ( TransactionHelper.toTransactionID(myStart)
                             -
                             TransactionHelper.toTransactionID(o.getStart()));
                }
            }
            else if (      !TransactionHelper.isTransactionID(myStart)
                        && TransactionHelper.isTransactionID(myEnd)
                        && !TransactionHelper.isTransactionID(o.getStart())
                        && TransactionHelper.isTransactionID(o.getEnd()))
           {
               // If they belong to the same transaction ?, this should
               // never happen.
               if (myEnd == o.getEnd()) {
                   return (int) (myStart - o.getStart());
               }
               else {
                   // Order them by thier transaction identifiers.
                   return (int)
                          ( TransactionHelper.toTransactionID(myEnd)
                            -
                            TransactionHelper.toTransactionID(o.getEnd()));
               }
            }
            else {
                // All other cases are considered equal.
                return 0;
            }
        }
        else {
            return comparableResult;
        }
    }

    /**
     * Returns the value of end
     */
    public long getEnd()
    {
        return myEnd;
    }

    /**
     * Returns the value of persitableKey
     */
    public K getPersitableKey()
    {
        return myPersitableKey;
    }

    /**
     * Returns the value of start
     */
    public long getStart()
    {
        return myStart;
    }

    /**
     * Setter for the end
     */
    public void setEnd(long end)
    {
        myEnd = end;
    }

    /**
     * Setter for the start
     */
    public void setStart(long start)
    {
        myStart = start;
    }
}
