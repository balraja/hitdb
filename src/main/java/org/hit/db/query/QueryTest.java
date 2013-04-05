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

package org.hit.db.query;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.CommonTree;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.hit.db.query.parser.MySQLLexer;
import org.hit.db.query.parser.MySQLParser;

/**
 * @author Balraja Subbiah
 */
public class QueryTest
{
    public static void printTree(CommonTree t, int indent) {
        if ( t != null ) {
            StringBuffer sb = new StringBuffer(indent);
            
            if (t.getParent() == null){
                System.out.println(sb.toString() + t.toString()); 
            }
            for ( int i = 0; i < indent; i++ )
                sb = sb.append("   ");
            for ( int i = 0; i < t.getChildCount(); i++ ) {
                System.out.println(sb.toString() + t.getChild(i).toString());
                printTree((CommonTree)t.getChild(i), indent+1);
            }
        }
    }
    
    public static void main(String[] args)
    {
        try {
            ANTLRStringStream fs = 
                new ANTLRStringStream("select * from climate_data");
            MySQLLexer lex = new MySQLLexer(fs);
            TokenRewriteStream tokens = new TokenRewriteStream(lex);
            MySQLParser parser = new MySQLParser(tokens);
            
            MySQLParser.data_manipulation_statements_return result = 
                parser.data_manipulation_statements();
            
            // WALK RESULTING TREE 
      
            // get tree from parser 
            // Create a tree node stream from resulting tree 
            CommonTree t = (CommonTree) result.getTree();
            printTree(t, 1);
            
        }
        catch (RecognitionException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
    }
}
