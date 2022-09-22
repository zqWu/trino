package io.trino.sql.rewritemv.predicate.visitor;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SelectItem;

import java.util.List;
import java.util.Map;


/**
 * mv 和 query 有着不同的 groupBy, 且query有having时, 才进行这样的改写
 * <p>
 * 改写 having Expression的 Visitor类
 * - column替换
 * - 函数支持
 */
public class HavingRewriteVisitor extends HavingVisitor {
    private static final Logger LOG = Logger.get(HavingRewriteVisitor.class);
    private final boolean isMvGrouped;

    public HavingRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                Map<Expression, QualifiedColumn> columnRefMap,
                                MvDetail mvDetail, boolean isMvGrouped) {
        super(mvSelectableColumnExtend, columnRefMap, mvDetail);
        this.isMvGrouped = isMvGrouped;
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, Void context) {
        if (!isMvGrouped) {
            // TODO: 如果mv自身没有进行groupBy, 那这些 having, 稍加改写就可以直接放出去
            return onMvNotGroupBy(node, context);
        }

        // mv自身进行了 groupBy, 对 having 需要做处理
        QualifiedName funName = node.getName();
        String name = funName.getSuffix();

        if (!SUPPORTED_FUNCTION.contains(name)) {
            return null;
        }
        switch (name) {
            // min/max/sum 符合这种特性
            // min(min(colA)) = min(colA)
            // sum(sum(colA)) = sum(colA)
            case "max":
            case "min":
            case "sum":
                return processOneArgFunction(node, node.getName());
            case "count":
                return processFunctionCount(node, node.getName());
            case "avg":
                // avg(colA) = sum( sum_A) / sum (cnt_A)
                List<Expression> arguments = node.getArguments();
                FunctionCall sumCountFun = new FunctionCall(FUNCTION_COUNT, arguments);
                Expression sumCount = processFunctionCount(sumCountFun, sumCountFun.getName());

                if (sumCount != null) {
                    FunctionCall sumValueFun = new FunctionCall(FUNCTION_SUM, arguments);
                    Expression sumValue = processOneArgFunction(sumValueFun, sumValueFun.getName());
                    if (sumValue != null) {
                        return new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE, sumValue, sumCount);
                    }
                }
                return null;
            default:
                return null;
        }
    }

    private Expression onMvNotGroupBy(FunctionCall node, Void context) {
        throw new UnsupportedOperationException("TODO");
    }

    /**
     * 直接查找
     * max/min/sum 有且只有一个 参数, 且为 column
     * <p>
     * mv: select max(colA) as max_colA
     * query: having max(colA) > 10
     * === rewrite to ===>
     * having max(max_colA) > 10
     *
     * @param funName max
     * @param node like FunctionCall("max", "price")
     */
    private Expression processOneArgFunction(FunctionCall node, QualifiedName funName) {
        List<Expression> arguments = node.getArguments();
        Expression arg1 = arguments.get(0);

        if (arg1 instanceof Identifier || arg1 instanceof DereferenceExpression) {
            // 在 mv中 查找 max(arg1) 这个selectable
            QualifiedColumn columnArg = origColumnRefMap.get(arg1);
            if (columnArg == null) {
                LOG.debug(String.format("mv中未找到需要的字段 %s", arg1));
                return null;
            }
            Expression expr = findAndRewriteSelectItemIfPossible(funName, columnArg);
            if (expr == null) {
                LOG.debug(String.format("having 无法处理 %s(%s)", funName.getSuffix(), columnArg));
                return null;
            }
            return new FunctionCall(funName, List.of(expr));
        } else {
            // 比如 having max(colA+colB) > 10 这种结构, 暂不支持
            LOG.warn("max function has complex expression, not support");
            return null;
        }
    }

    /**
     * count 有这些种: count(1), count(col), count(*)
     * 可能有0/1个参数
     * count(x) =======> sum(count_x)
     */
    private Expression processFunctionCount(FunctionCall node, QualifiedName funName) {
        List<Expression> arguments = node.getArguments();
        QualifiedColumn columnArg = null;
        if (arguments.size() == 0) { // case: count(*)
            columnArg = null;
        } else { // case: count(常数) or count(colA)
            Expression arg1 = arguments.get(0);
            if (arg1 instanceof Identifier || arg1 instanceof DereferenceExpression) {
                columnArg = origColumnRefMap.get(arg1);
                if (columnArg == null) {
                    LOG.debug(String.format("mv中未找到需要的字段 %s", arg1));
                    return null;
                }
            } else if (arg1 instanceof Literal) {
                columnArg = null;
            }
        }

        Expression expr = findAndRewriteSelectItemIfPossible(funName, columnArg);
        if (expr == null && columnArg != null) {
            // 刚刚尝试的是 count(colA), 找不到再试一下 col(*)
            expr = findAndRewriteSelectItemIfPossible(funName, null);
        }

        if (expr == null) {
            LOG.debug(String.format("having 无法处理 %s(%s)", funName.getSuffix(), columnArg));
            return null;
        }
        return new FunctionCall(FUNCTION_SUM, List.of(expr));
    }

}
