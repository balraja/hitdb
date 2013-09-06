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

package org.hit.query.test;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.List;

import org.antlr.runtime.RecognitionException;
import org.hit.db.model.Query;
import org.hit.db.query.operators.QueryBuildingException;
import org.hit.db.query.parser.QueryFactory;
import org.hit.example.Airport;
import org.hit.example.HitDbTest;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Balraja Subbiah
 */
public class QueryTest
{
    private TestDB myTestDB;
    
    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        myTestDB = new TestDB();
    }

    @Test
    public void testQueries() throws RecognitionException, QueryBuildingException
    {
        Query query = 
            QueryFactory.makeQuery("select * from " + HitDbTest.TABLE_NAME);
        @SuppressWarnings("unchecked")
        Collection<Airport> airports = 
            (Collection<Airport>) query.query(myTestDB);
        assertTrue(airports != null);
        assertFalse(airports.isEmpty());
    }
}
