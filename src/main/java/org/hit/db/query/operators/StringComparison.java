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
import java.util.regex.Pattern;

import org.hit.db.model.Row;
import org.hit.util.Range;

/**
 * Defines the contract for performing string comparision between 
 * columns.
 * 
 * @author Balraja Subbiah
 */
public class StringComparison implements Condition
{
    private String[] myColumnNames;
    
    private Pattern myPattern;
    
    /**
     * CTOR
     */
    public StringComparison()
    {
        myColumnNames = null;
        myPattern = null;
    }
    
    /**
     * CTOR
     */
    public StringComparison(String columnName, String regex)
    {
        myColumnNames = ColumnNameUtil.nestedColumnNames(columnName);
        myPattern = Pattern.compile(regex);
    }
    
    /**
     * CTOR
     */
    public StringComparison(String[] columnNames, Pattern pattern)
    {
        super();
        myColumnNames = columnNames;
        myPattern = pattern;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(Row record)
    {
        Object fieldValue = ColumnNameUtil.getValue(record, myColumnNames);
        return fieldValue != null ? myPattern.matcher(fieldValue.toString())
                                             .matches()
                                  : false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeObject(myColumnNames);
        out.writeUTF(myPattern.pattern());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readExternal(ObjectInput in)
        throws IOException, ClassNotFoundException
    {
        myColumnNames = (String[]) in.readObject();
        myPattern = Pattern.compile(in.readUTF());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <K extends Comparable<K>> void updateRange(Range<K> newRange)
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Condition cloneCondition() 
    {
        return new StringComparison(
            ColumnNameUtil.copyColumnName(myColumnNames), 
            Pattern.compile(myPattern.pattern()));
    }
}
