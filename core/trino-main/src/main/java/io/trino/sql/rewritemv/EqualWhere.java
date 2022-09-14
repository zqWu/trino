package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Literal;

import java.util.*;

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
    private static final Logger LOG = Logger.get(EqualWhere.class);

    private QualifiedSingleColumn colLeft;
    private QualifiedSingleColumn colRight;
    private Expression value1;
    private Expression value2;
    private int colCount = 0;
    private boolean notAlwaysTrue = true;

    // TODO 这个改成 static of方法
    public EqualWhere(ComparisonExpression expression,
                      Map<Expression, QualifiedSingleColumn> resolvedFieldMap) {
        requireNonNull(expression, "expression is null");

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

    public EqualWhere(Expression value1, Expression value2) {
        this.value1 = value1;
        this.value2 = value2;
        if (Objects.equals(value1, value2)) {
            notAlwaysTrue = false;
        }
        colCount = 0;
    }

    public EqualWhere(QualifiedSingleColumn colLeft, Expression value2) {
        this.colLeft = colLeft;
        this.value2 = value2;
        colCount = 1;
    }

    public EqualWhere(QualifiedSingleColumn colLeft, QualifiedSingleColumn colRight) {
        this.colLeft = colLeft;
        this.colRight = colRight;
        if (Objects.equals(colLeft, colRight)) {
            notAlwaysTrue = false;
        }
        colCount = 2;
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
        StringBuffer sb = new StringBuffer();
        if (colLeft != null) {
            sb.append(colLeft);
        } else {
            sb.append(value1);
        }
        sb.append("=");
        if (colRight != null) {
            sb.append(colRight);
        } else {
            sb.append(value2);
        }
        return sb.toString();
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

    /**
     * util
     * 等值 where的预处理
     */
    public static List<EqualWhere> preProcess(List<EqualWhere> in) {
        // ========= step: 过滤掉 true, 并把 equivalent Class 单独筛选出来
        List<EqualWhere> singleColumn = new ArrayList<>(in.size());
        List<EquivalentClass> ecList = new ArrayList<>(4);
        for (EqualWhere equalWhere : in) {
            if (!equalWhere.notAlwaysTrue()) { // 该条件一直为true, 比如 1=1这样
                continue;
            }

            if (equalWhere.getColCount() == 2) { // colA = colB, 构成了一个 equivalent class
                ecList.add(new EquivalentClass(equalWhere));
            } else { //
                singleColumn.add(equalWhere);
            }
        }

        // ========= step: 合并 equivalent Class
        // 比如 a=b c=d a=c 上面生成了 三个 ec, 需要合并成一个 a=b=c=d
        List<EquivalentClass> mergedEc = EquivalentClass.fullMerge(ecList);

        // ========= step:
        // 有 ec: a=b=c, e=f=g
        //   list: a=1, b=1, x=3
        // ===>
        //    ec: (1,a=b=c), (e=f=g), list: x=3
        //    list: x=3
        List<EqualWhere> result = new ArrayList<>(singleColumn.size() + 2);
        for (EqualWhere equalWhere : singleColumn) {
            QualifiedSingleColumn colLeft = equalWhere.getColLeft();
            if (colLeft == null) {
                result.add(equalWhere);
                continue;
            }
            boolean inEc = false;
            for (EquivalentClass ec : mergedEc) {
                if (ec.contain(colLeft)) {
                    if (!ec.setValueIfNecessary(equalWhere.getValue2())) {
                        // 比如 a=1 a=b b=2 这样的组合
                        LOG.warn(String.format("EquivalentClass 有2个不同的值 %s, %s", equalWhere.getValue2(), ec.getValue()));
                        throw new RuntimeException(String.format("EquivalentClass 有2个不同的值 %s, %s", equalWhere.getValue2(), ec.getValue()));
                    }
                    inEc = true;
                    break;
                }
            }
            if (!inEc) { // 比如 a=1, 且有一个 ec 对应了 a=b=c=d
                result.add(equalWhere);
            }
        }
        // ========= step:
        //    ec: (1,a=b=c), (e=f), list: x=3
        //    list: x=3
        // ==>
        //    list: x=3, a=1 b=1 c=1,  e=f, e=g

        for (EquivalentClass ec : mergedEc) {
            if (ec.getValue() != null) {
                Expression value = ec.getValue();
                for (QualifiedSingleColumn c : ec.getColumns()) {
                    result.add(new EqualWhere(c, value));
                }
            } else {
                List<QualifiedSingleColumn> columns = new ArrayList<>(ec.getColumns().size());
                columns.addAll(ec.getColumns());
                Collections.sort(columns);

                QualifiedSingleColumn min = columns.get(0);
                for (int i = 1; i < columns.size(); i++) {
                    result.add(new EqualWhere(min, columns.get(i)));
                }
            }
        }

        return result;
    }

}
