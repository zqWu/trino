package io.trino.sql.rewritemv.predicate.visitor;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.SelectItem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


/**
 * 改写 having Expression的 Visitor类
 * - column替换
 * - 函数支持
 */
public class HavingRewriteVisitor extends WhereColumnRewriteVisitor {
    private static final Logger LOG = Logger.get(HavingRewriteVisitor.class);
    private final boolean sameGroupBy;

    public HavingRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                Map<Expression, QualifiedColumn> columnRefMap,
                                MvDetail mvDetail,
                                boolean sameGroupBy) {
        super(mvSelectableColumnExtend, columnRefMap, mvDetail);
        this.sameGroupBy = sameGroupBy;
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, Void context) {
        List<Expression> arguments = node.getArguments();
        if (arguments == null || arguments.size() == 0) {
            // case: no argument function
            return node;
        }
        List<Expression> noConstantArgs = arguments.stream()
                .filter(e -> !(e instanceof Literal)).collect(Collectors.toList());
        if (noConstantArgs.size() == 0) {
            // case: all arguments is Literal
            return node;
        }

        String name = node.getName().getSuffix();
        if (SUPPORT_FUNCTION0.contains(name)) {
            // 这些函数 不能直接
            return null;
        }
        if (SUPPORT_FUNCTION1.contains(name)) {
            List<Expression> args2 = new ArrayList<>(arguments.size());
            arguments.forEach(arg -> args2.add(process(arg)));
            return new FunctionCall(node.getName(), args2);
        }
        return super.visitFunctionCall(node, context);
    }

    private void processFunctionMax() {

    }

    private final List<String> SUPPORT_FUNCTION0 = Arrays.asList("avg", "count");
    // 对于集合s1, s2, 满足: max( max(s1), max(s2)) = max( s1 + s2 ).
    // 对于集合s1, s2, 满足: min/sum
    private final List<String> SUPPORT_FUNCTION1 = Arrays.asList("max", "min", "sum");

}
