package io.trino.sql.rewritemv.predicate.visitor;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SelectItem;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * 改写 having Expression的 Visitor类
 * - column替换
 * - 函数支持
 */
public class HavingRewriteVisitor extends HavingVisitor {
    private static final Logger LOG = Logger.get(HavingRewriteVisitor.class);
    private static final List<String> SUPPORT_FUNCTION = Arrays.asList("max", "min", "sum", "avg", "count");

    public HavingRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                Map<Expression, QualifiedColumn> columnRefMap,
                                MvDetail mvDetail) {
        super(mvSelectableColumnExtend, columnRefMap, mvDetail);
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, Void context) {
        QualifiedName funName = node.getName();
        String name = funName.getSuffix();

        if (!SUPPORT_FUNCTION.contains(name)) {
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
            case "avg":
            case "count":
            default:
                return null;
        }
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

}
