package io.trino.sql.rewritemv.predicate;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;

import java.util.Objects;

public class PredicateOther
        extends Predicate
{
    public PredicateOther(Expression expr)
    {
        super(expr, PredicateType.OTHER);

        if (expr instanceof ComparisonExpression) {
            ComparisonExpression c = (ComparisonExpression) expr;
            ComparisonExpression.Operator op = c.getOperator();
            if (op == ComparisonExpression.Operator.EQUAL && Objects.equals(c.getLeft(), c.getRight())) {
                alwaysTrue = true;
            }
        }
    }
}
