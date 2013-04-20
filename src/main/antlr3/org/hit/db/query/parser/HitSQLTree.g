tree grammar HitSQLTree;

options 
{
    ASTLabelType = CommonTree;
    tokenVocab = HitSQL;
}

@header 
{
package org.hit.db.query.parser;

import com.google.common.collect.Lists;

import org.hit.db.query.operators.*;
import org.hit.util.Pair;

import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

}

@members
{
    QueryAttributes myQueryAttributes = new QueryAttributes();
    
    public QueryAttributes getQueryAttributes()
    {
        return myQueryAttributes;
    }
}

//column_name_expression

column_name returns [String coercedName]
scope {
    StringBuilder builder;
}
@init {
   $column_name::builder = new StringBuilder();
}
@after {
    coercedName = $column_name::builder.toString();
}
:^(COLUMN_NAME (ID {
    if ($column_name::builder.length() > 0) {
        $column_name::builder.append(".");
    }
    $column_name::builder.append($ID.text);
 } )+);

// expression
relational_op returns [NumericComparision.ComparisionOperator operator]: 
    EQ_SYM {$operator = NumericComparision.ComparisionOperator.EQ; }
    | LTH {$operator = NumericComparision.ComparisionOperator.LT;}
    | GTH {$operator= NumericComparision.ComparisionOperator.GT;}
    | NOT_EQ {$operator = NumericComparision.ComparisionOperator.NE;}
    | LET {$operator = NumericComparision.ComparisionOperator.LE;}
    | GET {$operator = NumericComparision.ComparisionOperator.GE;};
    
string_comparision_op : 
    LIKE_SYM | EQ_SYM | NOT_EQ;
conjunction_operators :
    AND_SYM | OR_SYM;

numeric_constant returns [double value] : 
    INTEGER_NUM {$value = Double.parseDouble($INTEGER_NUM.text);}
    | REAL_NUM {$value = Double.parseDouble($REAL_NUM.text);};

filtering_expression returns [Condition condition]   
: ^(r=relational_op c=column_name n=numeric_constant) {
    
    $condition = new NumericComparision($c.coercedName, $r.operator, $n.value);
    }
| ^(string_comparision_op  c=column_name  STRING) {
    $condition = new StringComparision($c.coercedName, $STRING.text);
  };

and_grouped_expression returns[Condition condition] 
scope {
    List<Condition> groupedConditions;
}
@init {
   $and_grouped_expression::groupedConditions = new ArrayList<Condition>();
}
@after {
    condition = new ConjugateCondition(ConjugateCondition.Conjunctive.AND,
                                       $and_grouped_expression::groupedConditions);
}
: ^(AND_SYM (f=filtering_expression{$and_grouped_expression::groupedConditions.add($f.condition);})+);

or_grouped_expression returns[Condition condition]
scope {
    List<Condition> groupedConditions;
}
@init {
   $or_grouped_expression::groupedConditions = new ArrayList<Condition>();
}
@after {
    condition = new ConjugateCondition(ConjugateCondition.Conjunctive.OR,
                                       $or_grouped_expression::groupedConditions);
}
: ^(OR_SYM (f=filtering_expression{$or_grouped_expression::groupedConditions.add($f.condition);})+);

expression_atom returns [Condition condition]:
    (f=filtering_expression { System.out.println("filtering");condition = $f.condition;})
    | (a=and_grouped_expression {condition= $a.condition;}) 
    | (o=or_grouped_expression {condition=$o.condition;});
    
expression returns[Condition condition]: 
    e=expression_atom {
        System.out.println("evaluating expression");
        condition = $e.condition;
    }
    | ^(c=conjunction_operators e1=expression_atom e2=expression_atom) {
        condition = 
            new ConjugateCondition(
                $c.text, Lists.<Condition>newArrayList($e1.condition, $e2.condition));
};
    
// select ------  http://dev.mysql.com/doc/refman/5.6/en/select.html  -------------------------------

select_statement:
 ^(SELECT select_list ^(FROM table_references) where_clause? groupby_clause? having_clause? orderby_clause? limit_clause?);

where_clause: ^(WHERE e=expression) {
    System.out.println("where");
    myQueryAttributes.setWhereCondition($e.condition);
};

groupby_clause
scope {
    List<String> columnNames;
}
@init {
    $groupby_clause::columnNames = new ArrayList<>();
}
@after {
    myQueryAttributes.setGroupByAttributes($groupby_clause::columnNames );
}
: ^(GROUPING_COLUMNS (c=column_name{$groupby_clause::columnNames.add(
    $c.coercedName);})+);

having_clause: ^(HAVING e=expression) {myQueryAttributes.setHavingCondition(
$e.condition);};

orderby_clause
scope {
    Map<String, Boolean> orderByCollector;
}
@init {
   $orderby_clause::orderByCollector = new HashMap<>();
}
@after {
   myQueryAttributes.setOrderByCriterion(
       $orderby_clause::orderByCollector);
}
:^(ORDERED_COLUMNS orderby_item+);

order returns [Boolean ascending] : (ASC {$ascending=Boolean.TRUE;}) 
                                  | (DSC {$ascending=Boolean.FALSE;});

orderby_item:  ^(ORDERED_COLUMN c=column_name (o=order)?) {
$orderby_clause::orderByCollector.put($c.coercedName, $o.ascending);} ;

limit_clause: ^(LIMIT INTEGER_NUM) {
    myQueryAttributes.setLimit(Integer.parseInt($INTEGER_NUM.text));
};

select_list
scope {
    Map<String, Aggregate.ID> columnCollector;
}
@init {
    $select_list::columnCollector = new HashMap<>();
}
@after {
    if (!$select_list::columnCollector.isEmpty()) {
        myQueryAttributes.setSelectedColumns($select_list::columnCollector);
    }
    else {
        myQueryAttributes.setSelectedColumns(
            Collections.<String, Aggregate.ID>singletonMap("ALL", null));
    }
}
: ^(SELECTED_COLUMNS column_ref+) 
| ^(SELECTED_COLUMNS ALL);

group_function:
    AVG | COUNT | MAX_SYM | MIN_SYM | SUM;
    
column_ref :
     c=column_name {$select_list::columnCollector.put($c.coercedName, null);}
     | ^(GROUPED_COLUMN g=group_function c=column_name {
            $select_list::columnCollector.put($c.coercedName, 
                                              Aggregate.ID.valueOf($g.text));
        });

column_list:
    LPAREN column_name (COMMA column_name)* RPAREN;

table_references: ID {
                      myQueryAttributes.setTableName($ID.text);
                  }  
                  | table_cross_product 
                  | table_join;

table_cross_product
scope {
    List<String> nameList;
}
@init {
    $table_cross_product::nameList= new ArrayList<>();
}
@after {
    myQueryAttributes.setTableCrossProduct($table_cross_product::nameList);
}
: ^(CROSSED_TABLES (ID {
    $table_cross_product::nameList.add($ID.text);
 })+);
     
table_join
scope {
    List<String> nameList;
}
@init {
    $table_join::nameList= new ArrayList<>(); 
    Condition joinCondition = null;
}
@after {
    myQueryAttributes.setJoinCriteria(new Pair<>(
        $table_join::nameList, joinCondition));
}
: ^(JOINED_TABLES (ID{
    $table_join::nameList.add($ID.text);
    })+ 
    (c=join_condition{joinCondition = $c.condition;}));

join_condition returns [Condition condition]: ^(ON e=expression) {
    condition = $e.condition;
};
