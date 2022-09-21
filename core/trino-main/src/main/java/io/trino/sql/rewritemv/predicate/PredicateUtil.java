package io.trino.sql.rewritemv.predicate;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.rewritemv.RewriteUtils;
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
import io.trino.sql.tree.NotExpression;
import io.trino.sql.tree.SelectItem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PredicateUtil {
    /**
     * range 条件处理:
     * (1) every condition in loose, must also in tight
     * (2) (originalRange - mvRange) can be rewrite
     *
     * @param originalRange 严格条件, = original 的 range 条件
     * @param mvRange 宽松条件, = mv的 range 条件
     * @param ecList 处理需要的ec
     * @param compensation 严格条件 - 宽松条件 得到的补偿条件
     * @return null if all ok, error if loose not cover tight
     */
    public static String rangePredicateCompare(
            List<PredicateRange> originalRange,
            List<PredicateRange> mvRange,
            List<EquivalentClass> ecList,
            List<Predicate> compensation) {

        List<PredicateRange> looseRemain = new ArrayList<>(mvRange);
        List<PredicateRange> tightRemain = new ArrayList<>(originalRange);

        // only support conditions based on ec
        for (EquivalentClass ec : ecList) {

            // find all predicate based on ec
            List<PredicateRange> tightPredicate = originalRange.stream()
                    .filter(pr -> ec.getColumns().contains(pr.getLeft()))
                    .collect(Collectors.toList());

            List<PredicateRange> loosePredicate = mvRange.stream()
                    .filter(pr -> ec.getColumns().contains(pr.getLeft()))
                    .collect(Collectors.toList());

            if (tightPredicate.size() == 0) {
                if (loosePredicate.size() == 0) {
                    continue;
                } else {
                    return "range predicate: cannot cover: " + loosePredicate.get(0);
                }
            }
            looseRemain.removeAll(loosePredicate);
            tightRemain.removeAll(tightPredicate);

            QualifiedColumn column = ec.getColumns().stream().findAny().get();
            PredicateRange shouldSmallRange = PredicateRange.unbound(column, ec);
            for (PredicateRange predicateRange : tightPredicate) {
                shouldSmallRange = shouldSmallRange.intersection(predicateRange);
            }

            PredicateRange shouldLargeRange = PredicateRange.unbound(column, ec);
            for (PredicateRange predicateRange : loosePredicate) {
                shouldLargeRange = shouldLargeRange.intersection(predicateRange);
            }
            if (!shouldLargeRange.coverOtherValue(shouldSmallRange)) {
                return "range predicate: cannot cover: " + shouldLargeRange;
            }
            PredicateRange comp = shouldSmallRange.baseOn(shouldLargeRange);
            if (comp.getEqual() != PredicateRange.PredicateRangeBound.UNBOUND
                    || comp.getLower() != PredicateRange.PredicateRangeBound.UNBOUND
                    || comp.getUpper() != PredicateRange.PredicateRangeBound.UNBOUND) {
                compensation.add(comp);
            }
        }

        if (looseRemain.size() != 0) {
            return "range predicate: condition not in ec (loose):" + looseRemain.get(0).toString();
        }

        if (tightRemain.size() != 0) {
            return "range predicate: condition not in ec (tight)" + tightRemain.get(0).toString();
        }

        return null;
    }


    /**
     * 比较ec 并得到补偿条件
     *
     * @param origEcList = original ec list
     * @param mvEcList = mv ec list
     * @param compensation = 改写时 补偿条件
     * @return null if all ok, error if loose not cover tight
     */
    public static String processPredicateEqual(
            List<EquivalentClass> origEcList,
            List<EquivalentClass> mvEcList,
            List<Predicate> compensation) {

        // make sure all equal in mv, must appear in query
        // 1.1 EquivalentClass contain, eg,
        // mv   eg=[ (colA, colB),       (colC, colD),       (colM, comN)]
        // orig eg=[ (colA, colB, colE), (colC, colD, colY), (colM)      ]
        Map<EquivalentClass, List<EquivalentClass>> map = new HashMap<>(); // ec in original ---- 多个关联ec in mv


        // every non-trivial ec in mv, must in original
        for (EquivalentClass mvEc : mvEcList) {
            if (mvEc.getColumns().size() < 2) {
                continue;
            }

            QualifiedColumn oneColumnInEc = mvEc.getColumns().stream().findAny().get();

            Optional<EquivalentClass> origEcOpt = origEcList.stream().filter(x -> x.contain(oneColumnInEc)).findAny();
            if (origEcOpt.isEmpty()) {
                return ("predicate: mv has equal predicate(s) not exists in query - 1");
            }
            EquivalentClass origEc = origEcOpt.get();

            if (!origEc.getColumns().containsAll(mvEc.getColumns())) {
                // eg (colM, colN) --- (colM)
                return ("predicate: mv has equal predicate(s) not exists in query - 2");
            }

            if (map.get(origEc) == null) {
                List<EquivalentClass> list = new ArrayList<>();
                list.add(mvEc);
                map.put(origEc, list);
            } else {
                map.get(origEc).add(mvEc);
            }
        }

        // 处理剩下的 ec条件
        List<EquivalentClass> tmp1 = new ArrayList<>(origEcList.size());
        for (EquivalentClass origEc : origEcList) {
            List<EquivalentClass> toRemove = map.get(origEc);
            if (toRemove == null || toRemove.size() == 0) {
                tmp1.add(origEc);
            } else {
                // 从 origEc中 删除 toRemove中保留的关系
                EquivalentClass remain = new EquivalentClass();
                Set<QualifiedColumn> big = new HashSet<>();
                for (EquivalentClass ecToRemove : toRemove) {
                    QualifiedColumn each = ecToRemove.getColumns().stream().findAny().get();
                    remain.add(each);
                    big.addAll(ecToRemove.getColumns());
                }

                for (QualifiedColumn c : origEc.getColumns()) {
                    if (!big.contains(c)) {
                        remain.add(c);
                    }
                }
                tmp1.add(remain);
            }
        }

        for (EquivalentClass ec : tmp1) {
            if (ec.getColumns().size() >= 2) {
                // 只有一个元素的 EquivalentClassBase是没有 = 关系的 {colA}
                Set<QualifiedColumn> columns = ec.getColumns();
                List<QualifiedColumn> list = new ArrayList<>(columns);
                QualifiedColumn col0 = list.get(0);
                for (int i = 1; i < list.size(); i++) {
                    compensation.add(new PredicateEqual(null, col0, list.get(i)));
                }
            }
        }

        return null;
    }

    public static String processPredicateOther(
            List<PredicateOther> queryOtherList,
            List<PredicateOther> mvOtherList,
            List<Expression> compensation,

            Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
            Map<Expression, QualifiedColumn> columnRefMap,
            MvDetail mvDetail
    ) {
        List<Expression> queryOther = new ArrayList<>();
        String err1 = replaceColumnInCondition(queryOtherList, queryOther, mvSelectableColumnExtend, columnRefMap, mvDetail);
        if (err1 != null) {
            return err1;
        }

        List<Expression> mvOther = new ArrayList<>();
        String err2 = replaceColumnInCondition(mvOtherList, mvOther, mvSelectableColumnExtend, columnRefMap, mvDetail);
        if (err2 != null) {
            return err2;
        }

        for (Expression expr : queryOther) {
            if (mvOther.contains(expr)) {
                mvOther.remove(expr);
            } else {
                compensation.add(expr);
            }
        }
        return null;
    }

    private static String replaceColumnInCondition(
            List<PredicateOther> other,
            List<Expression> replaced,
            Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
            Map<Expression, QualifiedColumn> columnRefMap,
            MvDetail mvDetail
    ) {
        if (other == null || other.size() == 0) {
            return null;
        }

        for (PredicateOther predicate : other) {
            if (predicate.isAlwaysTrue()) {
                continue;
            }

            Expression before = predicate.getExpr();
            ColumnRewriteVisitor rewriter = new ColumnRewriteVisitor(mvSelectableColumnExtend, columnRefMap, mvDetail);
            Expression after = rewriter.process(before);
            if (after != null) {
                replaced.add(after);
            } else {
                return ("predicate: could not handle other predicate" + before);
            }
        }

        return null;
    }

    /**
     * 改写 Expression的 Visitor类
     * 目前是 column替换
     */
    private static class ColumnRewriteVisitor extends AstVisitor<Expression, Void> {
        private static final Logger LOG = Logger.get(ColumnRewriteVisitor.class);
        private final Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend;
        private final Map<Expression, QualifiedColumn> columnRefMap;
        private final MvDetail mvDetail;

        public ColumnRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                    Map<Expression, QualifiedColumn> columnRefMap, MvDetail mvDetail) {
            this.mvSelectableColumnExtend = mvSelectableColumnExtend;
            this.columnRefMap = columnRefMap;
            this.mvDetail = mvDetail;
        }

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
}
