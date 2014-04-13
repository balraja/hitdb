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

import org.hit.db.transactions.PhasedTransactionExecutor.Phase;
import org.hit.pool.Poolable;
import org.hit.pool.PooledObjects;

/** 
 * The class that stores the intermediate state between phases 
 */
public class Memento<T> implements Poolable
{
    private AbstractTransaction myTransaction;
    
    private Phase<T> myPhase;
    
    /**
     * CTOR
     */
    public static <T> Memento<T> create(AbstractTransaction transaction, 
                                        Phase<T> phase)
    {
        @SuppressWarnings("unchecked")
        Memento<T> memento = 
            PooledObjects.getInstance(Memento.class);
        memento.myTransaction = transaction;
        memento.myPhase = phase;
        return memento;
    }

    /**
     * Returns the value of transaction
     */
    public AbstractTransaction getTransaction()
    {
        return myTransaction;
    }

    /**
     * Returns the value of phase
     */
    public PhasedTransactionExecutor.Phase<T> getPhase()
    {
        return myPhase;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void free()
    {
    }
}