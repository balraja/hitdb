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
package org.hit.client.command;

import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Defines the contract for a type that can be used for formatting the
 * result on the client.
 * 
 * @author Balraja Subbiah
 */
public class Formater
{
    public static Formater ourCachedInstance = null;
    
    public static Formater getInstance()
    {
        if (ourCachedInstance == null) {
            ourCachedInstance = new Formater();
        }
        return ourCachedInstance;
    }
    
    private final NumberFormat myNumberFormat;
    
    private final DateFormat myDateFormat;
    
    /**
     * CTOR
     */
    public Formater()
    {
        this(new DecimalFormat("0.###E0"), 
             new SimpleDateFormat("yyyy-MM-dd@HHmmss@Z"));
    }
    
    /**
     * CTOR
     */
    public Formater(NumberFormat numberFormat, DateFormat dateFormat)
    {
        myNumberFormat = numberFormat;
        myDateFormat = dateFormat;
    }
    
    /**
     * Returns the formatted value of result.
     */
    public String format(Object result)
    {
        if (result instanceof Number) {
            return myNumberFormat.format(result);
        }
        else if (result instanceof Date) {
            return myDateFormat.format(result);
        }
        else {
            return result.toString();
        }
    }
}
