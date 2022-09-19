package io.trino.sql.rewritemv.where;

import io.trino.sql.tree.Expression;

/**
 * atomic where = an expression which result is true / false
 */
public class AtomicWhere {
    public enum WhereType {
        COLUMN_EQUAL,           // predicate equal, colA = colB
        COLUMN_RANGE,           // predicate range, colA > 3
        LITERAL_EQUAL,          // 3 = 2
        OTHER                   // other, a like '%x', colA is not null
    }

    // always true的条件可以去掉, 比如  1=1, colA=colA
    protected boolean alwaysTrue = false;
    protected boolean alwaysFalse = false;
    protected final Expression expr;
    protected final WhereType whereType;

    public AtomicWhere(Expression expr, WhereType whereType) {
        this.expr = expr;
        this.whereType = whereType;
    }

    // ========get

    public boolean isAlwaysFalse() {
        return alwaysFalse;
    }

    public boolean isAlwaysTrue() {
        return alwaysTrue;
    }

    public Expression getExpr() {
        return expr;
    }

    public WhereType getWhereType() {
        return whereType;
    }

}
