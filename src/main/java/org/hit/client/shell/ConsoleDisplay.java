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
package org.hit.client.shell;

import java.util.Collection;

import org.hit.client.command.Display;
import org.hit.client.command.Formater;
import org.hit.db.model.Row;

/**
 * Implementation of {@link Display} where the results are published to 
 * the console.
 * 
 * @author Balraja Subbiah
 */
public class ConsoleDisplay implements Display
{
    /**
     * {@inheritDoc}
     */
    @Override
    public void publishLine(String line)
    {
        System.out.println(line);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publishRows(String query, Collection<Row> rows)
    {
        if (rows.isEmpty()) {
            System.out.println("EMPTY RESULT");
        }
        else {
            Row firstRow = rows.iterator().next();
            Collection<String> fieldNames = firstRow.getFieldNames();
            StringBuilder builder = new StringBuilder();
            boolean isFirst = true;
            for (String fieldName : fieldNames) {
                if (isFirst) {
                    isFirst = false;
                }
                else {
                    builder.append("\t");
                }
                builder.append(fieldName);
            }
            System.out.println(builder.toString());
            System.out.println("\n");
            for (Row row : rows) {
                printRow(row, fieldNames, builder);
            }
        }
    }
    
    private void printRow(Row                row, 
                          Collection<String> fieldNames,
                          StringBuilder      builder) 
    {
        Formater formater = Formater.getInstance();
        builder.setLength(0);
        boolean isFirst = true;
        for (String fieldName : fieldNames) {
            if (isFirst) {
                isFirst = false;
            }
            else {
                builder.append("\t");
            }
            builder.append(formater.format(row.getFieldValue(fieldName)));
        }
        System.out.println(builder.toString());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void publishError(Throwable throwable)
    {
        throwable.printStackTrace();
    }
}
