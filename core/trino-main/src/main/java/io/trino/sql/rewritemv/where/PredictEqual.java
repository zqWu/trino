package io.trino.sql.rewritemv.where;

import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.Expression;

import java.util.Objects;

/**
 * expression: colA = colB
 * 排序较小的作为 left, 排序较大的作为 right
 */
public class PredictEqual extends AtomicWhere {
    private final QualifiedColumn left;
    private final QualifiedColumn right;

    public PredictEqual(Expression expr, QualifiedColumn left, QualifiedColumn right) {
        super(expr, WhereType.COLUMN_EQUAL);

        if (left.compareTo(right) > 0) {
            this.left = right;
            this.right = left;
        } else {
            this.left = left;
            this.right = right;
        }
        if (Objects.equals(left, right)) {
            alwaysTrue = true;
        }
    }

    @Override
    public String toString() {
        return left + "=" + right;
    }

    // ======== getter
    public QualifiedColumn getLeft() {
        return left;
    }

    public QualifiedColumn getRight() {
        return right;
    }
}
