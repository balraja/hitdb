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

package org.hit.db.query.operators;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

import org.hit.db.model.Row;

/**
 * Defines the contract for condition list, joined by conjunctive operators.
 * 
 * @author Balraja Subbiah
 */
public class ConjugateCondition implements Condition
{
    public static enum Conjunctive 
    {
        AND,
        OR;
        
        static Conjunctive getConjunctive(String conjunction)
        {
            for (Conjunctive conj : values()) {
                if (conjunction.equalsIgnoreCase(conj.name())) {
                    return conj;
                }
            }
            return null;
        }
    }
    
    private List<Condition> myConditions;
    
    private Conjunctive myConjunctive;
    
    /**
     * CTOR
     */
    public ConjugateCondition()
    {
        myConjunctive = null;
        myConditions = null;
    }
    
    /**
     * CTOR
     */
    public ConjugateCondition(String conjunctive, List<Condition> conditions)
    {
        this(Conjunctive.getConjunctive(conjunctive), conditions);
    }

    /**
     * CTOR
     */
    public ConjugateCondition(Conjunctive     conjunctive,
                              List<Condition> conditions)
    {
        super();
        myConditions = conditions;
        myConjunctive = conjunctive;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(Row record)
    {
        if (myConjunctive == Conjunctive.AND) {
            for (Condition condition : myConditions) {
                if (!condition.isValid(record)){
                    return false;
                }
            }
            return true;
        }
        else {
            for (Condition condition : myConditions) {
                if (condition.isValid(record)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeUTF(myConjunctive.name());
        out.writeObject(myConditions);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myConjunctive = Conjunctive.valueOf(in.readUTF());
        myConditions = (List<Condition>) in.readObject();
    }
}
