grammar HitSQL;

options 
{
    language=Java;
    output=AST;
    backtrack=true;
}

tokens {
    SELECTED_COLUMNS;
    ALL;
    GROUPING_COLUMNS;
    GROUPED_COLUMN;
    ORDERED_COLUMNS;
    ORDERED_COLUMN;
    TABLE;
    CROSSED_TABLES;
    JOINED_TABLES;
    COLUMN_NAME;
}

@header 
{
package org.hit.db.query.parser;
}

@lexer::header 
{
package org.hit.db.query.parser;
}

fragment A_ :   'a' | 'A';
fragment B_ :   'b' | 'B';
fragment C_ :   'c' | 'C';
fragment D_ :   'd' | 'D';
fragment E_ :   'e' | 'E';
fragment F_ :   'f' | 'F';
fragment G_ :   'g' | 'G';
fragment H_ :   'h' | 'H';
fragment I_ :   'i' | 'I';
fragment J_ :   'j' | 'J';
fragment K_ :   'k' | 'K';
fragment L_ :   'l' | 'L';
fragment M_ :   'm' | 'M';
fragment N_ :   'n' | 'N';
fragment O_ :   'o' | 'O';
fragment P_ :   'p' | 'P';
fragment Q_ :   'q' | 'Q';
fragment R_ :   'r' | 'R';
fragment S_ :   's' | 'S';
fragment T_ :   't' | 'T';
fragment U_ :   'u' | 'U';
fragment V_ :   'v' | 'V';
fragment W_ :   'w' | 'W';
fragment X_ :   'x' | 'X';
fragment Y_ :   'y' | 'Y';
fragment Z_ :   'z' | 'Z';

ABS             : A_ B_ S_ ;
ALL             : A_ L_ L_  ;
ASC             : A_ S_ C_  ;
AVG             : A_ V_ G_;
BY_SYM          : B_ Y_ ;
COUNT           : C_ O_ U_ N_ T_;
DESC            : D_ E_ S_ C_  ;
FROM                : F_ R_ O_ M_  ;
GROUP_SYM           : G_ R_ O_ U_ P_  ;
HAVING         : H_ A_ V_ I_ N_ G_  ;
LIMIT               : L_ I_ M_ I_ T_ ;
IN_SYM              : I_ N_  ;
JOIN_SYM            : J_ O_ I_ N_  ;
LIKE_SYM            : L_ I_ K_ E_; 
MAX_SYM             : M_ A_ X_  ;
MIN_SYM             : M_ I_ N_  ;
ORDER_SYM           : O_ R_ D_ E_ R_;
ON                  : O_ N_;
SELECT              : S_ E_ L_ E_ C_ T_ ;
SUM                 : S_ U_ M_;
USING               : U_ S_ I_ N_ G_;
WHERE               : W_ H_ E_ R_ E_  ;


DIVIDE  : (  D_ I_ V_ ) | '/' ;
MOD_SYM : (  M_ O_ D_ ) | '%' ;
OR_SYM  : (  O_ R_ ) | '||';
AND_SYM : (  A_ N_ D_ ) | '&&';

ARROW   : '=>' ;
EQ_SYM  : '=' | '<=>' ;
NOT_EQ  : '<>' | '!=' | '~='| '^=';
LET : '<=' ;
GET : '>=' ;
SET_VAR : ':=' ;
SHIFT_LEFT  : '<<' ;
SHIFT_RIGHT : '>>' ;
ALL_FIELDS  : '.*' ;

SEMI    : ';' ;
COLON   : ':' ;
DOT : '.' ;
COMMA   : ',' ;
ASTERISK: '*' ;
RPAREN  : ')' ;
LPAREN  : '(' ;
RBRACK  : ']' ;
LBRACK  : '[' ;
PLUS    : '+' ;
MINUS   : '-' ;
NEGATION: '~' ;
VERTBAR : '|' ;
BITAND  : '&' ;
POWER_OP: '^' ;
GTH : '>' ;
LTH : '<' ;
fragment DIGIT  : '0' .. '9';
INTEGER_NUM     : DIGIT+ ;
REAL_NUM        : (DIGIT)* DOT INTEGER_NUM;
fragment QUOTE  : '\'' | '\"';
STRING          : QUOTE(~( QUOTE | '\\' ) |  . )*QUOTE;
fragment U_CHAR : 'A'..'Z';
fragment L_CHAR : 'a'..'z';
fragment CHAR   : U_CHAR | L_CHAR;
fragment ID_CHAR : CHAR | '_';
ID:(ID_CHAR)+;
fragment SPACE: ' ' | '\t';
WHITESPACE: SPACE+ { $channel = HIDDEN; };

//column_name_expression
group_function:
    AVG | COUNT | MAX_SYM | MIN_SYM | SUM;
table_name  : ID;
column_name : (ID DOT)* ID -> ^(COLUMN_NAME ID+);
aggr_column_name : group_function LPAREN column_name RPAREN {System.out.println("Aggr column name");} 
       -> column_name;

// expression
relational_op: 
    EQ_SYM | LTH | GTH | NOT_EQ | LET | GET;
string_comparision_op : 
    LIKE_SYM | EQ_SYM | NOT_EQ;
conjunction_operators :
    AND_SYM | OR_SYM;

numeric_constant : INTEGER_NUM | REAL_NUM;
column : column_name | aggr_column_name;
numeric_cmp_expression : column relational_op numeric_constant
  -> ^(relational_op column numeric_constant) ;
column_cmp_expression : column_name relational_op column_name
  -> ^(relational_op column_name column_name) ;
string_cmp_expression  : column_name string_comparision_op^ STRING;
filtering_expression   : 
    numeric_cmp_expression | string_cmp_expression | column_cmp_expression;
and_grouped_expression : 
    LPAREN filtering_expression (AND_SYM filtering_expression)+ RPAREN
    -> ^(AND_SYM filtering_expression+);
or_grouped_expression : 
    LPAREN filtering_expression (OR_SYM filtering_expression)+ RPAREN
    -> ^(OR_SYM filtering_expression+);
expression_atom :
    filtering_expression | and_grouped_expression | or_grouped_expression;
expression : 
    expression_atom (conjunction_operators^ expression_atom)*;
    
// select ------  http://dev.mysql.com/doc/refman/5.6/en/select.html  -------------------------------

select_statement: 
    SELECT select_list FROM table_references 
    ( 
        ( where_clause )? 
        ( groupby_clause )?
        ( having_clause )?
    ) ?
    ( orderby_clause )?
    ( limit_clause )?
-> ^(SELECT select_list ^(FROM table_references) where_clause? groupby_clause? having_clause? orderby_clause? limit_clause?);

where_clause: WHERE expression -> ^(WHERE expression);

groupby_clause: GROUP_SYM BY_SYM column_name (COMMA column_name)* 
    -> ^(GROUPING_COLUMNS column_name+);

having_clause:HAVING expression -> ^(HAVING expression);

orderby_clause:ORDER_SYM BY_SYM orderby_item (COMMA orderby_item)* -> 
   ^(ORDERED_COLUMNS orderby_item+);

order : ASC | DESC;
orderby_item: column_name (order)? -> ^(ORDERED_COLUMN column_name order?) ;

limit_clause: LIMIT INTEGER_NUM -> ^(LIMIT INTEGER_NUM);

select_list:
    (column_ref ( COMMA column_ref )*) -> ^(SELECTED_COLUMNS column_ref+)
    | ASTERISK -> ^(SELECTED_COLUMNS ALL);

column_ref :
     column_name
     | group_function LPAREN column_name RPAREN 
       -> ^(GROUPED_COLUMN group_function column_name);

column_list:
    LPAREN column_name (COMMA column_name)* RPAREN;

table_references: table_name  
                  | table_cross_product 
                  | table_join;

table_cross_product: table_name (COMMA table_name)* -> ^(CROSSED_TABLES table_name+);
     
table_join: table_name (JOIN_SYM table_name)+ join_condition
   -> ^(JOINED_TABLES table_name+ join_condition);

join_condition: ON expression -> ^(ON expression);
