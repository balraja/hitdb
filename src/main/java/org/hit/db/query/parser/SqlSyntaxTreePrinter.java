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

/**
 * @author Balraja Subbiah
 */
public class SqlSyntaxTreePrinter
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
                new ANTLRStringStream(
                    "select max(id) from climate_data");
            
            HitSQLLexer lex = new HitSQLLexer(fs);
            TokenRewriteStream tokens = new TokenRewriteStream(lex);
            HitSQLParser parser = new HitSQLParser(tokens);
            
            HitSQLParser.select_statement_return result = 
                parser.select_statement();
            
            CommonTree t = (CommonTree) result.getTree();
            printTree(t, 1);
            
            CommonTreeNodeStream nodeStream = new CommonTreeNodeStream(t);
            HitSQLTree tree = new HitSQLTree(nodeStream);
            tree.select_statement();
            System.out.println(tree.getQueryAttributes());
            
        }
        catch (RecognitionException e) {
            e.printStackTrace();
        } 
    }
}
