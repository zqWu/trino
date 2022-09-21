package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.predicate.AtomicWhere;
import io.trino.sql.rewritemv.predicate.PredicateAnalysis;
import io.trino.sql.rewritemv.predicate.PredicateEqual;
import io.trino.sql.rewritemv.predicate.PredicateOther;
import io.trino.sql.rewritemv.predicate.PredicateRange;
import io.trino.sql.rewritemv.predicate.PredicateUtil;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.AstVisitor;
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
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.SelectItem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

/**
 * rewrite where条件
 */
public class WhereRewriter {
    private static final Logger LOG = Logger.get(WhereRewriter.class);
    private final QueryRewriter queryRewriter;
    private final QuerySpecificationRewriter specRewriter;
    private final Map<Expression, QualifiedColumn> columnRefMap;
    private final MvDetail mvDetail;
    private final PredicateAnalysis mvPredicateAnalysis;
    private PredicateAnalysis wherePredicate;

    // 在ec加持下, mv可选字段. 注意: 这个对象不是 mv持有
    private Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend;

    public WhereRewriter(QuerySpecificationRewriter specRewriter, MvDetail mvDetail) {
        this.specRewriter = specRewriter;
        this.queryRewriter = specRewriter.getQueryRewriter();
        this.mvDetail = mvDetail;
        this.columnRefMap = specRewriter.getColumnRefMap();

        mvPredicateAnalysis = mvDetail.getWherePredicate();
    }

    /**
     * 返回改写后的 where
     */
    public Expression process() {
        Optional<Expression> optWhere = queryRewriter.getSpec().getWhere();
        Optional<Expression> mvWhere = mvDetail.getQuerySpecification().getWhere();

        // case: query where=null
        if (optWhere.isEmpty()) {
            if (mvWhere.isPresent()) {
                if (mvPredicateAnalysis.hasEffectivePredicate()) {
                    // mv包含有效的 where过滤, 则无法匹配
                    notFit("where(original) = empty,  where(mv) != empty");
                }
            }
            return null;
        }

        if (!mvPredicateAnalysis.isSupport()) {
            notFit("where: mv not support, reason=" + mvPredicateAnalysis.getReason());
            return null;
        }

        wherePredicate = RewriteUtils.analyzeWhere(optWhere.get(), specRewriter.getColumnRefMap());
        if (!wherePredicate.isSupport()) {
            notFit("where: original not support, reason=" + wherePredicate.getReason());
        }

        return compareAndRewriteWhere();
    }

    /**
     * 根据 whereAnalysis 和 mvDetail.mvAnalysis 处理 where
     * 1. 所有 mvWhereAnalysis中的条件, 在 originalWhereAnalysis 都有
     * 2. originalWhereAnalysis 多下来的条件, 需要适当的改写
     * where a=b vs where a=1 and b=1, 虽然后者能够推到前者, 但是目前不做这方面工作
     */
    private Expression compareAndRewriteWhere() {
        // === 处理equal条件
        List<AtomicWhere> compensation = new ArrayList<>();
        String errProcessEqual = PredicateUtil.processPredicateEqual(
                wherePredicate.getEcList(),
                mvDetail.getWherePredicate().getEcList(),
                compensation
        );
        if (errProcessEqual != null) {
            notFit("where: " + errProcessEqual);
            return null;
        }

        // !!! ec处理完毕后, 就可以更新在mv中的 selectable字段
        mvSelectableColumnExtend = RewriteUtils.extendSelectableColumnByEc(mvDetail.getSelectableColumn(), wherePredicate.getEcList());

        List<AtomicWhere> list2 = new ArrayList<>();
        String errProcessRange = PredicateUtil.rangePredicateCompare(wherePredicate.getRangeList(),
                mvDetail.getWherePredicate().getRangeList(),
                wherePredicate.getEcList(),
                list2);
        if (errProcessRange != null) {
            notFit("where: " + errProcessRange);
            return null;
        }

        compensation.addAll(list2);

        // 整理 compensation
        List<Expression> conditions = parseAtomicWhere(compensation);

        // 3. 剩下的 predicate 处理
        List<Expression> list3 = processPredicateOther();
        conditions.addAll(list3);

        if (conditions == null || conditions.size() == 0) {
            return null;
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            return new LogicalExpression(LogicalExpression.Operator.AND, conditions);
        }
    }

    /**
     * 处理剩下的条件, 非 equal / range
     *
     * @return 补偿条件
     */
    private List<Expression> processPredicateOther() {
        List<Expression> queryOther = preProcess(wherePredicate.getOtherList());
        List<Expression> mvOthers = preProcess(mvPredicateAnalysis.getOtherList());


        List<Expression> compensation = new ArrayList<>();
        for (Expression expr : queryOther) {
            if (mvOthers.contains(expr)) {
                mvOthers.remove(expr);
            } else {
                compensation.add(expr);
            }
        }

        return compensation;
    }

    private List<Expression> parseAtomicWhere(List<AtomicWhere> compensation) {
        List<Expression> list = new ArrayList<>(compensation.size() + 3);

        for (AtomicWhere atomicWhere : compensation) {
            if (atomicWhere instanceof PredicateEqual) {
                PredicateEqual pe = (PredicateEqual) atomicWhere;
                QualifiedColumn left = pe.getLeft();
                QualifiedColumn right = pe.getRight();
                DereferenceExpression mvLeft = RewriteUtils.findColumnInMv(left, mvDetail.getSelectableColumn(), mvDetail.getTableNameExpression());
                DereferenceExpression mvRight = RewriteUtils.findColumnInMv(right, mvDetail.getSelectableColumn(), mvDetail.getTableNameExpression());
                if (mvLeft == null) {
                    notFit("where: cannot find column in mv:" + left);
                    return list;
                }
                if (mvRight == null) {
                    notFit("where: cannot find column in mv:" + right);
                    return list;
                }
                list.add(new ComparisonExpression(EQUAL, mvLeft, mvRight));
            } else if (atomicWhere instanceof PredicateRange) {
                PredicateRange pr = (PredicateRange) atomicWhere;
                DereferenceExpression columnInMv = RewriteUtils.findColumnInMv(pr.getLeft(), mvSelectableColumnExtend, mvDetail.getTableNameExpression());
                if (pr.getEqual() != PredicateRange.PredicateRangeBound.UNBOUND) {
                    list.add(new ComparisonExpression(EQUAL, columnInMv, pr.getEqual().getValue()));
                }
                if (pr.getLower() != PredicateRange.PredicateRangeBound.UNBOUND) {
                    list.add(new ComparisonExpression(pr.getLower().getOp(), columnInMv, pr.getLower().getValue()));
                }
                if (pr.getUpper() != PredicateRange.PredicateRangeBound.UNBOUND) {
                    list.add(new ComparisonExpression(pr.getUpper().getOp(), columnInMv, pr.getUpper().getValue()));
                }
            } else {
                // PredicateOther
                PredicateOther other = (PredicateOther) atomicWhere;
                // 需要替换 column
                LOG.debug("TODO PredicateOther 条件的处理");
            }
        }

        return list;
    }

    /**
     * TODO: 这里的处理需要加强, 如果 expression 自身不包含 col,但是 child包含, 则怎么处理
     * colA + 3 > 4
     * 要用 AstVisitor进行处理
     * <p>
     * 处理 other 类型的 predicate
     * - 移除 True条件
     * - column 替换, 使用mv中的column进行替换
     */
    private List<Expression> preProcess(List<PredicateOther> other) {
        if (other == null || other.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        List<Expression> replaced = new ArrayList<>(other.size());

        for (PredicateOther predicate : other) {
            if (predicate.isAlwaysTrue()) {
                continue;
            }

            Expression before = predicate.getExpr();
            ColumnRewriteVisitor rewriter = new ColumnRewriteVisitor();
            Expression after = rewriter.process(before);
            if (after != null) {
                replaced.add(after);
            } else {
                notFit("where: could not handle other predicate" + before);
                break;
            }
        }

        return replaced;
    }


    /**
     * 改写 PredicateOther的 visitor类
     */
    private class ColumnRewriteVisitor extends AstVisitor<Expression, Void> {

        private DereferenceExpression getColumnReference(QualifiedColumn col) {
            return RewriteUtils.findColumnInMv(col, mvSelectableColumnExtend, mvDetail.getTableNameExpression());
        }

        private void __notSupport(Expression node) {
            LOG.warn("not support:" + node);
        }

        @Override
        protected Expression visitIdentifier(Identifier node, Void context) {
            QualifiedColumn col = columnRefMap.get(node);
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


    }

    // ======== get
    private boolean isMvFit() {
        return queryRewriter.isMvFit();
    }

    public void notFit(String reason) {
        queryRewriter.notFit(reason);
    }

    public PredicateAnalysis getWherePredicate() {
        return wherePredicate;
    }

    public Map<QualifiedColumn, SelectItem> getMvSelectableColumnExtend() {
        return mvSelectableColumnExtend;
    }
}
