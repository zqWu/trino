package io.trino.sql.rewritemv.predicate.visitor;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.SelectItem;

import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * 改写 having Expression的 Visitor类
 * - column替换
 * - 函数支持
 */
public class HavingToWhereRewriteVisitor extends WhereColumnRewriteVisitor {
    private static final Logger LOG = Logger.get(HavingToWhereRewriteVisitor.class);

    public HavingToWhereRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                       Map<Expression, QualifiedColumn> columnRefMap,
                                       MvDetail mvDetail) {
        super(mvSelectableColumnExtend, columnRefMap, mvDetail);
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, Void context) {
        String name = node.getName().getSuffix();
        if (!SUPPORT_FUNCTION.contains(name)) {
            return null;
        }
        switch (name) {
            case "max":
                return processFunctionMax(node);
            default:
                // TODO
                return null;
        }
    }

    private Expression processFunctionMax(FunctionCall node) {
        List<Expression> arguments = node.getArguments();
        Expression arg1 = arguments.get(0);
        if (arg1 instanceof Identifier || arg1 instanceof DereferenceExpression) {
            // 在 mv中 查找 max(arg1) 这个selectable
        }
        LOG.warn("max function has complex expression, not support");
        return null;
    }


    private final List<String> SUPPORT_FUNCTION = Arrays.asList("avg", "count", "max", "min", "sum");
}
