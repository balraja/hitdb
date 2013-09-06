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

package org.hit.db.query.parser;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.hit.db.model.Query;
import org.hit.db.query.operators.QueryBuilder;
import org.hit.db.query.operators.QueryBuildingException;

/**
 * An util class that can be used for parsing query string and generating 
 * <code>Query</code> out of it.
 * 
 * @author Balraja Subbiah
 */
public final class QueryFactory
{
    /**
     * Parses the given string to generate query out of it.
     */
    public static Query makeQuery(String query) 
        throws RecognitionException, QueryBuildingException
    {
        ANTLRStringStream fs = new ANTLRStringStream(query);
        HitSQLLexer lex = new HitSQLLexer(fs);
        TokenRewriteStream tokens = new TokenRewriteStream(lex);
        HitSQLParser parser = new HitSQLParser(tokens);

        HitSQLParser.select_statement_return result =
            parser.select_statement();

        CommonTree t = (CommonTree) result.getTree();
        CommonTreeNodeStream nodeStream = new CommonTreeNodeStream(t);
        HitSQLTree tree = new HitSQLTree(nodeStream);
        tree.select_statement();
        QueryBuilder builder = new QueryBuilder(tree.getQueryAttributes());
        return builder.buildQuery();
    }
    
    /**
     * Private CTOR to avoid initialization
     */
    private QueryFactory()
    {
    }
}
