package io.trino.sql.rewritemv.rewriter;

import io.airlift.log.Logger;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LogicalExpression;

import java.util.*;

import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

/**
 * rewrite where条件
 */
class WhereRewriter {
    private static final Logger LOG = Logger.get(WhereRewriter.class);
    private final QuerySpecRewriter querySpecRewriter;
    private final Optional<Expression> originalWhere;
    private final Optional<Expression> mvWhere;

    public WhereRewriter(QuerySpecRewriter querySpecRewriter) {
        this.querySpecRewriter = querySpecRewriter;
        originalWhere = querySpecRewriter.getOriginalSpec().getWhere();
        mvWhere = querySpecRewriter.getMvSpec().getWhere();
    }

    public Optional<Expression> process() {
        if (originalWhere.isEmpty()) {
            if (mvWhere.isPresent()) {
                notFit("where(original) = empty,  where(mv) != empty");
            }
            return originalWhere;
        }

        Expression origExpr = originalWhere.get();
        Expression mvExpr = null;
        if (mvWhere.isPresent()) {
            mvExpr = mvWhere.get();
        }

        Expression expr = compareAndRewriteWhere(origExpr, mvExpr);
        if (expr == null) {
            return Optional.empty();
        }
        return Optional.of(expr);
    }

    /**
     * 比较 where中的 expression, 并进行改写
     * 复杂 where 比较, 目前支持方式:
     * <li>
     * 1. 都是 and 方式
     * 2. originalSpec 中的 条件更多
     * originalSpec: where a=1 and b=2 and kk.c=3
     * mvSpec      : where a=1 and b=2
     * 3. mvSpec中缺少的 where条件(kk.c=3), 在其结果集(select字段)中 存在
     * </li>
     *
     * @param orig 原 sql中的 where, 非null
     * @param mv mv中的 where, 可能null
     * @return 改写后的 where
     */
    private Expression compareAndRewriteWhere(Expression orig, Expression mv) {
        // 1. 检测 mv中的条件, orig都有
        // 2. 对 orig中有但是 mv中没有的条件, 可以转化

        // 先把所有的 where expression 打平
        List<EqualWhere> origRecords = new ArrayList<>();
        if (!flattenAndNormalize(orig, origRecords, querySpecRewriter.getOriginalColumnRefMap())) {
            notFit("original sql where无法处理");
            return null;
        }
        List<EqualWhere> mvRecords = new ArrayList<>();
        if (!flattenAndNormalize(mv, mvRecords, querySpecRewriter.getMvColumnRefMap())) {
            notFit("mv sql where无法处理");
            return null;
        }

        List<EqualWhere> orig2 = EqualWhere.preProcess(origRecords);
        List<EqualWhere> mv2 = EqualWhere.preProcess(mvRecords);

        List<EqualWhere> same = new ArrayList<>();
        // 1. 检测 mv中的条件, orig都有
        for (EqualWhere whereInMv : mv2) {
            int index = orig2.indexOf(whereInMv);
            if (index == -1) {
                notFit("该条件在 mv中出现, 但是在 original中没有:" + whereInMv.toString());
                return null;
            }
            orig2.remove(index);
            same.add(whereInMv);
        }

        // 现在orig2中的都是 original中有, 但是mv中没有的条件, 检测这些条件 并增加到 statement中
        List<Expression> remainItem = new ArrayList<>(orig2.size());
        for (EqualWhere remain : orig2) {
            if (remain.getColCount() == 0) { // 这个是个无关 col的形式
                ComparisonExpression expr = new ComparisonExpression(EQUAL, remain.getValue1(), remain.getValue2());
                remainItem.add(expr);
                continue;
            }

            DereferenceExpression newLeft = querySpecRewriter.correspondColumnInMv(remain.getColLeft());
            if (newLeft == null) {
                notFit("original中的 where条件 无法通过改写满足" + remain.getColLeft().toString());
                return null;
            }
            // 处理 right表达式
            Expression newRight = remain.getValue2(); // 默认值
            if (remain.getColRight() != null) {
                newRight = querySpecRewriter.correspondColumnInMv(remain.getColRight());
                if (newRight == null) {
                    notFit("original中的 where条件 无法通过改写满足" + remain.getColRight().toString());
                    return null;
                }
            }

            ComparisonExpression oneWhereAfterRewrite = new ComparisonExpression(EQUAL, newLeft, newRight);
            remainItem.add(oneWhereAfterRewrite);
        }

        if (remainItem.size() == 0) {
            return null;
        } else if (remainItem.size() == 1) {
            return remainItem.get(0);
        } else {
            LogicalExpression logicalExpression = new LogicalExpression(LogicalExpression.Operator.AND, remainItem);
            return logicalExpression;
        }
    }

    /**
     * 预处理 where 条件
     *
     * @return true = 支持后续处理, false = 当前的where无法处理
     */
    private static boolean flattenAndNormalize(Expression orig,
                                               List<EqualWhere> records,
                                               Map<Expression, QualifiedSingleColumn> map) {
        List<Expression> exprList = new ArrayList<>();
        if (!flattenExpression(orig, exprList)) {
            LOG.debug("original中的where不是 conjunctive形式, 暂不支持");
            return false;
        }
        for (Expression expr : exprList) {
            if (!whereExpressionSupportCheck(expr)) {
                LOG.debug("where 条件中, 目前仅支持 = 条件, 当前表达式=" + expr.toString());
                continue;
            }
            EqualWhere record = new EqualWhere((ComparisonExpression) expr, map);
            records.add(record);
        }
        return true;
    }

    private boolean isMvFit() {
        return querySpecRewriter.isMvFit();
    }

    public void notFit(String reason) {
        querySpecRewriter.notFit(reason);
    }

    /**
     * util
     * 将多个 expression 打平, 用于后续比较
     * 目前进支持的类型是: conjunctive形式, 如
     * 1. expr
     * 2. expr and expr
     * 3. expr and (expr and expr)
     * 不支持
     * 1. expr or expr
     *
     * @return true=符合要求的 expr, false=输入不符合要求
     */
    private static boolean flattenExpression(Expression expr, List<Expression> flat) {
        if (expr == null) {
            return true;
        }

        boolean isConjunctive = true;
        Vector<Expression> tmp = new Vector<>(6);
        tmp.add(expr);

        while (!tmp.isEmpty()) {
            Expression remove = tmp.remove(0);
            if (!(remove instanceof LogicalExpression)) {
                flat.add(remove); // 单个 expr 直接放到 flat中
            } else {
                LogicalExpression logicExpr = (LogicalExpression) remove;
                LogicalExpression.Operator op = logicExpr.getOperator();
                if (op == LogicalExpression.Operator.AND) {
                    tmp.addAll(logicExpr.getTerms());
                } else {
                    LOG.warn("目前仅支持 AND logic operator");
                    isConjunctive = false;
                    break;
                }
            }
        }
        return isConjunctive;
    }

    /**
     * util
     * 目前仅支持 equal条件, 也就是 a=b, c=d 这种形式
     */
    private static boolean whereExpressionSupportCheck(Expression expr) {
        boolean support = false;
        if (expr instanceof ComparisonExpression) {
            ComparisonExpression c = (ComparisonExpression) expr;
            if (c.getOperator() == EQUAL) {
                support = true;
            }
        }
        return support;
    }


}
