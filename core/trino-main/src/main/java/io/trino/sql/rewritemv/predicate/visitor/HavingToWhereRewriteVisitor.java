package io.trino.sql.rewritemv.predicate.visitor;

import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
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
 * mv 和 query 有着相同的 groupBy, 且query有having时, 才进行这样的改写
 * <p>
 * 改写 having Expression的 Visitor类
 * rewrite 后 这些会并入 where 条件中
 * - column替换
 * - 函数支持
 */
public class HavingToWhereRewriteVisitor extends HavingVisitor {
    public HavingToWhereRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                       Map<Expression, QualifiedColumn> columnRefMap,
                                       MvDetail mvDetail) {
        super(mvSelectableColumnExtend, columnRefMap, mvDetail);
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, Void context) {
        QualifiedName funName = node.getName();
        String name = funName.getSuffix();

        if (!SUPPORTED_FUNCTION.contains(name)) {
            return null;
        }
        switch (name) {
            case "max":
            case "min":
            case "sum":
                return processOneArgFunction(node, node.getName());
            case "avg":
                Expression result = processOneArgFunction(node, node.getName());
                if (result == null) {
                    // TODO, avg 直接查不到
                    // 通过 sum / count 进行查找
                    LOG.debug("avg current not support sum/count ");
                }
                return result;
            case "count":
                return processFunctionCount(node, node.getName());
            default:
                return null;
        }
    }

    /**
     * count 有这些种: count(1), count(col), count(*)
     * 可能有0/1个参数
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
        if (expr == null) {
            LOG.debug(String.format("having 无法处理 %s(%s)", funName.getSuffix(), columnArg));
        }
        return expr;
    }

    /**
     * 直接查找
     * max/min/sum/avg 有且只有一个 参数, 且为 column
     * having max(colA) > 10  ======>  where max_colA > 10
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
            }
            return expr;
        } else {
            // 比如 having max(colA+colB) > 10 这种结构, 暂不支持
            LOG.warn("max function has complex expression, not support");
            return null;
        }
    }
}
