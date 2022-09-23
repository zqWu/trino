package io.trino.sql.rewritemv.predicate.visitor;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.rewritemv.RewriteUtils;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.ExistsPredicate;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InListExpression;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.LikePredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.SelectItem;

import java.util.List;
import java.util.Map;


/**
 * 改写 where 中的 Expression的 Visitor类
 * 目前是 column替换
 * TODO 如果某些没有 node没有change, 是不需要 new 的
 */
public class WhereColumnRewriteVisitor extends ExpressionRewriter {
    private static final Logger LOG = Logger.get(WhereColumnRewriteVisitor.class);
    protected final Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend;
    protected final Map<Expression, QualifiedColumn> origColumnRefMap;
    protected final MvDetail mvDetail;

    public WhereColumnRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                     Map<Expression, QualifiedColumn> origColumnRefMap, MvDetail mvDetail) {
        this.mvSelectableColumnExtend = mvSelectableColumnExtend;
        this.origColumnRefMap = origColumnRefMap;
        this.mvDetail = mvDetail;
    }

    protected DereferenceExpression getColumnReference(QualifiedColumn col) {
        return RewriteUtils.findColumnInMv(col, mvSelectableColumnExtend, mvDetail.getTableNameExpression());
    }

    @Override
    protected Expression visitIdentifier(Identifier node, Void context) {
        QualifiedColumn col = origColumnRefMap.get(node);
        return getColumnReference(col);
    }

    @Override
    protected Expression visitLiteral(Literal node, Void context) {
        return node;
    }

    @Override
    protected Expression visitInListExpression(InListExpression node, Void context) {
        // 目前仅处理  in (val1, val2) 这样的形式, 如果 in (select ... ) 则不处理
        List<Expression> values = node.getValues();
        boolean allLiteral = values.stream().allMatch(v -> (v instanceof Literal));
        if (allLiteral) {
            return node;
        }

        __notSupport(node);
        return null;
    }

    @Override
    protected Expression visitLikePredicate(LikePredicate node, Void context) {
        Expression expr = process(node.getValue());
        if (expr == null) {
            return null;
        }
        return new LikePredicate(expr, node.getPattern(), node.getEscape());
    }

    @Override
    protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
        Expression expr = process(node.getValue());
        if (expr == null) {
            return null;
        }
        return new IsNotNullPredicate(expr);
    }

    @Override
    protected Expression visitIsNullPredicate(IsNullPredicate node, Void context) {
        Expression expr = process(node.getValue());
        if (expr == null) {
            return null;
        }
        return new IsNullPredicate(expr);
    }

    @Override
    protected Expression visitExists(ExistsPredicate node, Void context) {
        LOG.warn("not support:" + node);
        return null;
    }

    @Override
    protected Expression visitInPredicate(InPredicate node, Void context) {

        Expression newValue = process(node.getValue());
        InListExpression inListExpr = (InListExpression) process(node.getValueList());
        if (newValue != null && inListExpr != null) {
            return new InPredicate(newValue, inListExpr);
        }

        __notSupport(node);
        return null;
    }

    @Override
    protected Expression visitNotExpression(NotExpression node, Void context) {
        Expression value = node.getValue();
        Expression newExpr = process(value);
        if (newExpr == null) {
            return null;
        }
        return new NotExpression(newExpr);
    }

    @Override
    protected Expression visitComparisonExpression(ComparisonExpression node, Void context) {
        ComparisonExpression.Operator op = node.getOperator();

        Expression newLeft = process(node.getLeft());
        Expression newRight = process(node.getRight());
        if (newLeft != null && newRight != null) {
            return new ComparisonExpression(op, newLeft, newRight);
        }

        __notSupport(node);
        return null;
    }

    @Override
    protected Expression visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
        ArithmeticBinaryExpression.Operator op = node.getOperator();
        if (node.getLeft() instanceof Literal && node.getRight() instanceof Literal) {
            return node;
        }
        Expression newLeft = process(node.getLeft());
        Expression newRight = process(node.getRight());

        if (newLeft != null && newRight != null) {
            return new ArithmeticBinaryExpression(op, newLeft, newRight);
        }
        __notSupport(node);
        return null;
    }

    @Override
    protected Expression visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
        __notSupport(node);
        return null;
    }

    /**
     * 有些表达式 不属于上面 几种, 目前暂不支持
     */
    @Override
    protected Expression visitExpression(Expression node, Void context) {
        // TODO
        __notSupport(node);
        return null;
    }

    protected void __notSupport(Expression node) {
        LOG.warn("not support:" + node);
    }
}
