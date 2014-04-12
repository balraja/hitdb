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
import org.hit.pool.PoolConfiguration;
import org.hit.pool.PooledObjects;

/**
 * Implements {@link TransactionValidator} to validate whether the updates
 * made by a transaction can be committed.
 * 
 * @author Balraja Subbiah
 */
@PoolConfiguration(size=10000,initialSize=100)
public class WriteTransactionValidator extends ReadTransactionValidator
{
    /**
     * CTOR
     */
    public static WriteTransactionValidator create(
        TransactableDatabase database,
        long                 validationTime,
        long                 transactionId)
    {
        WriteTransactionValidator validator = 
            PooledObjects.getInstance(WriteTransactionValidator.class);
        ReadTransactionValidator.initialize(
            validator, database, validationTime, transactionId);
        return validator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>, P extends Persistable<K>> boolean
        isAcceptable(TransactionTableTrail<K, P> trail)
    {
        boolean readValidation = super.isAcceptable(trail);
        
        if (!readValidation) {
            return false;
        }
        else {
            for (Transactable<K,P> transactable : trail.getWriteSet()) {
                if (!transactable.validate(getValidationTime(),
                                           getTransactionId()).isValid())
                {
                    return false;
                }
            }
            for (Transactable<K,P> transactable : trail.getNewWriteSet()) {
                if (!transactable.validate(getValidationTime(),
                                           getTransactionId()).isValid())
                {
                    return false;
                }
            }
            return true;
        }
    }
}
