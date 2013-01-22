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

package org.hit.facade;

import org.hit.db.model.Mutation;

/**
 * Defines the contract for the call back interface to be supplied 
 * along with a mutation that can be used for notifying its success and 
 * failure.
 * 
 * @author Balraja Subbiah
 */
public interface MutationCallback
{
    /** This method will be called on success */
    public void onSuccess(Mutation mutation);
    
    /** The handler to be called on failure */
    public void onFailure(Mutation mutation, String message, Throwable exception);
}