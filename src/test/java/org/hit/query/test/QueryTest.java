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

import org.antlr.runtime.RecognitionException;
import org.hit.db.model.Query;
import org.hit.db.model.Row;
import org.hit.db.query.operators.ColumnNameUtil;
import org.hit.db.query.operators.QueryBuildingException;
import org.hit.db.query.operators.RowMap;
import org.hit.db.query.parser.QueryParser;
import org.hit.example.Airport;
import org.hit.example.HitDbTest;
import org.junit.Before;
import org.junit.Test;

/**
 * Testcases for verifying the correctness of query execution.
 * 
 * @author Balraja Subbiah
 */
public class QueryTest
{
    private TestDB myTestDB;
    
    /**
     * Sets up the artifacts for testing.
     */
    @Before
    public void setUp() throws Exception
    {
        myTestDB = new TestDB();
    }

    /**
     * Tests the select * query.
     */
    @Test
    public void testSelect() throws RecognitionException, QueryBuildingException
    {
        Query query = 
            QueryParser.parseQuery("select * from " + HitDbTest.TABLE_NAME);
        
        @SuppressWarnings("unchecked")
        Collection<Airport> airports = 
            (Collection<Airport>) query.query(myTestDB);
        assertTrue(airports != null);
        assertFalse(airports.isEmpty());
        assertEquals(7411, airports.size());
    }
    
    /**
     * Tests the select count(*) query.
     */
    @Test
    public void testCount() throws RecognitionException, QueryBuildingException
    {
        Query query = 
            QueryParser.parseQuery(
                "select count(*) from " + HitDbTest.TABLE_NAME);
        
        @SuppressWarnings("unchecked")
        Collection<Row> result = 
            (Collection<Row>) query.query(myTestDB);
        assertEquals(1, result.size());
        
        Row cntResult = result.iterator().next();
        assertNotNull(cntResult);
        
        assertEquals(7411.0D,
                     cntResult.getFieldValue(ColumnNameUtil.ALL_COLUMNS_SYMBOLIC));
    }
    
    /**
     * Tests the select max(column_name) query.
     */
    @Test
    public void testMax() throws RecognitionException, QueryBuildingException
    {
        Query query = 
            QueryParser.parseQuery(
                "select max(id) from " + HitDbTest.TABLE_NAME);
        
        @SuppressWarnings("unchecked")
        Collection<Row> result = 
            (Collection<Row>) query.query(myTestDB);
        assertEquals(1, result.size());
        
        Row cntResult = result.iterator().next();
        Double maxID = (Double) cntResult.getFieldValue("id");
        assertEquals(8844L, maxID.longValue());
    }
    
    /**
     * Tests the select max(column_name) query.
     */
    @Test
    public void testJoin() throws RecognitionException, QueryBuildingException
    {
        Query query = 
            QueryParser.parseQuery(
               "select count(*) " +
               "from airports join routes " +
               "on airports.id = routes.src_airport_id" +
               " where airports.id = 3093");
                    
        @SuppressWarnings("unchecked")
        Collection<Row> result = 
            (Collection<Row>) query.query(myTestDB);
        
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(1, result.size());
        
        Row firstRow = result.iterator().next();
        assertEquals(4.0D, 
                     firstRow.getFieldValue(ColumnNameUtil.ALL_COLUMNS_SYMBOLIC));
    }
}
