package io.trino.sql.rewritemv.rewriter;

import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * where 条件中的 =, 假设有下面形式, 以及对等形式 (等式左右互换)
 * col1 = 常量,                   如 age = 3 ======================> 支持
 * col1 = col2,                  如 a.id = b.aid =================> 支持
 * 常量 = 常量,                   如 1 = 1 ========================> 支持
 * col1 = fun(常量),              如 year = getYear(一个日期常量) ===> 不支持
 * col1 = fun(字段),              如 year = getYear(某个字段)    ===> 不支持
 * fun(col1) = fun(col2),        如 trim(col1) = trim(col2)    ===> 不支持
 */
class EqualWhere {
    private final ComparisonExpression expression;
    private QualifiedSingleColumn colLeft;
    private QualifiedSingleColumn colRight;
    private Expression value1;
    private Expression value2;
    private int colCount = 0;
    private boolean notAlwaysTrue = true;

    public EqualWhere(ComparisonExpression expression,
                      Map<Expression, QualifiedSingleColumn> resolvedFieldMap) {
        requireNonNull(expression, "expression is null");

        this.expression = expression;

        if (ComparisonExpression.Operator.EQUAL != expression.getOperator()) {
            throw new UnsupportedOperationException("必须是 EQUAL 表达式");
        }

        value1 = expression.getLeft();
        if (resolvedFieldMap.get(value1) != null) { // 包含字段
            colLeft = resolvedFieldMap.get(value1);
            value1 = null;
            colCount++;
        } else if (!(value1 instanceof Literal)) {
            throw new UnsupportedOperationException("目前仅支持字面量");
        }

        value2 = expression.getRight();
        if (resolvedFieldMap.get(value2) != null) {
            colRight = resolvedFieldMap.get(value2);
            value2 = null;
            colCount++;
        } else if (!(value2 instanceof Literal)) {
            throw new UnsupportedOperationException("目前仅支持字面量");
        }

        if (colCount > 0) {
            if (colLeft == null) {
                colLeft = colRight;
                colRight = null;

                value2 = value1;
                value1 = null;
            }

            if (colRight != null && colLeft.compareTo(colRight) > 0) {
                // 这种情况下, value1 = value2 = null
                QualifiedSingleColumn tmp = colLeft;
                colLeft = colRight;
                colRight = tmp;
            }
        }

        // 该条件是否 always true
        if (colCount == 0 && Objects.equals(value1, value2)) {
            notAlwaysTrue = false; // always equal
        } else if (colCount == 2 && colLeft.compareTo(colRight) == 0) {
            notAlwaysTrue = false; // always equal
        }
    }

    public ComparisonExpression getExpression() {
        return expression;
    }

    public QualifiedSingleColumn getColLeft() {
        return colLeft;
    }

    public QualifiedSingleColumn getColRight() {
        return colRight;
    }

    public int getColCount() {
        return colCount;
    }

    public Expression getValue1() {
        return value1;
    }

    public Expression getValue2() {
        return value2;
    }

    public boolean notAlwaysTrue() {
        return notAlwaysTrue;
    }

    @Override
    public String toString() {
        return expression.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EqualWhere)) return false;
        EqualWhere record = (EqualWhere) o;
        if (colCount != record.colCount) {
            return false;
        }
        if (colCount == 0) {
            // 仅有 value 相等
            return (Objects.equals(value1, record.value1) && Objects.equals(value2, record.value2))
                    || (Objects.equals(value1, record.value2) && Objects.equals(value2, record.value1));
        } else if (colCount == 1) {
            return (Objects.equals(colLeft, record.colLeft)) && Objects.equals(value2, record.value2);
        } else {
            return (Objects.equals(colLeft, record.colLeft)) && (Objects.equals(colRight, record.colRight));
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(colLeft, colRight, colCount);
    }
}
