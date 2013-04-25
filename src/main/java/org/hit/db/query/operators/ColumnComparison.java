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

import org.hit.db.model.Queryable;

/**
 * Defines the contract for comparing two columns.
 * 
 * @author Balraja Subbiah
 */
public class ColumnComparison implements Condition
{
    private String[] myColumnNames1;
    
    private String[] myColumnNames2;
    
    private ComparisionOperator myOperator;
    
    /**
     * CTOR
     */
    public ColumnComparison()
    {
        myColumnNames1 = null;
        myColumnNames2 = null;
        myOperator = null;
    }
    
    /**
     * CTOR
     */
    public ColumnComparison(
        ComparisionOperator op, String columnName1, String columnName2)
    {
        myOperator = op;
        myColumnNames1 = ColumnNameUtil.nestedColumnNames(columnName1);
        myColumnNames2 = ColumnNameUtil.nestedColumnNames(columnName2);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(Queryable record)
    {
        Object fieldValue1 = ColumnNameUtil.getValue(record, myColumnNames1);
        Object fieldValue2 = ColumnNameUtil.getValue(record, myColumnNames2);
        
        if (fieldValue1 != null && fieldValue2 != null) {
            if (   fieldValue1 instanceof Number
                && fieldValue2 instanceof Number)
            {
                return myOperator.compare(
                    ((Number) fieldValue1).doubleValue(),
                    ((Number) fieldValue2).doubleValue());
            }
            else {
                return myOperator.compare(fieldValue1, fieldValue2);
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myColumnNames1);
        out.writeObject(myColumnNames2);
        out.writeUTF(myOperator.name());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myColumnNames1 = (String[]) in.readObject();
        myColumnNames2 = (String[]) in.readObject();
        myOperator = ComparisionOperator.valueOf(in.readUTF());
    }
}
