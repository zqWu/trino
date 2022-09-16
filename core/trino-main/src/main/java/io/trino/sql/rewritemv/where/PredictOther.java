package io.trino.sql.rewritemv.where;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;

import java.util.Objects;

public class PredictOther extends AtomicWhere {
    public PredictOther(Expression expr) {
        super(expr, WhereType.OTHER);

        if (expr instanceof ComparisonExpression) {
            ComparisonExpression c = (ComparisonExpression) expr;
            if (Objects.equals(c.getLeft(), c.getRight())) {
                alwaysTrue = true;
            }
        }
    }

}
