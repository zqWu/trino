package io.trino.sql.rewritemv.predicate;

import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.rewritemv.predicate.visitor.ExpressionRewriter;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PredicateUtil {

    /**
     * 多个 condition 使用 and进行组装
     */
    public static Expression logicAnd(List<Expression> conditions) {
        if (conditions == null || conditions.size() == 0) {
            return null;
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            return new LogicalExpression(LogicalExpression.Operator.AND, conditions);
        }
    }

    public static Expression logicAnd(Expression expr1, Expression expr2) {
        return new LogicalExpression(LogicalExpression.Operator.AND, Arrays.asList(expr1, expr2));
    }

    /**
     * flatten where to atomic predicate
     */
    public static PredicateAnalysis analyzePredicate(Expression whereExpr, Map<Expression, QualifiedColumn> map) {
        if (whereExpr == null) {
            return PredicateAnalysis.EMPTY_PREDICATE;
        }

        PredicateAnalysis whereAnalysis = new PredicateAnalysis();
        PredicateVisitor visitor = new PredicateVisitor(map);
        visitor.process(whereExpr, whereAnalysis);
        whereAnalysis.build();

        return whereAnalysis;
    }

    /**
     * 将一个 大的 predicate 分解为多个 {@link Predicate}
     */
    private static class PredicateVisitor extends AstVisitor<Void, PredicateAnalysis> {
        private final Map<Expression, QualifiedColumn> refMap;

        public PredicateVisitor(Map<Expression, QualifiedColumn> refMap) {
            this.refMap = refMap;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, PredicateAnalysis context) {
            // TODO 对于比较, 需要区分是 colA op colB, 还是 colA op value
            // colA=3这样的形式
            Expression left = node.getLeft();
            Expression right = node.getRight();
            ComparisonExpression.Operator op = node.getOperator();
            boolean equalOp = ComparisonExpression.Operator.EQUAL == op;

            // c.s.tableA.colX = 1, colA<1 , 1 >= c.s.tableA.colX,  1=colA
            if ((left instanceof DereferenceExpression || left instanceof Identifier) && right instanceof Literal) {
                if (PredicateRange.validOperator(op) && PredicateRange.validLiteral((Literal) right)) {
                    QualifiedColumn column1 = refMap.get(left);
                    context.addPredicate(new PredicateRange(node, column1, (Literal) right, op));
                } else {
                    context.addPredicate(new PredicateOther(node));
                }
            } else if ((right instanceof DereferenceExpression || right instanceof Identifier) && left instanceof Literal) {
                if (PredicateRange.validOperator(op) && PredicateRange.validLiteral((Literal) left)) {
                    QualifiedColumn column1 = refMap.get(right);
                    context.addPredicate(new PredicateRange(node, column1, (Literal) left, op));
                } else {
                    context.addPredicate(new PredicateOther(node));
                }
            }

            // colA=colB, colB=c.s.tableA.colY,  c.s.tableA.colX=colB, c.s.tableA.colX=c.s.tableA.colY
            else if (equalOp
                    && (left instanceof Identifier || left instanceof DereferenceExpression)
                    && (right instanceof Identifier || right instanceof DereferenceExpression)
            ) {
                QualifiedColumn column1 = refMap.get(left);
                QualifiedColumn column2 = refMap.get(right);
                context.addPredicate(new PredicateEqual(node, column1, column2));
            }

            // other situation
            else {
                context.addPredicate(new PredicateOther(node));
            }

            return null;
        }

        @Override
        protected Void visitBetweenPredicate(BetweenPredicate node, PredicateAnalysis context) {
            Expression value = node.getValue();
            Expression min = node.getMin();
            Expression max = node.getMax();
            if ((value instanceof DereferenceExpression || value instanceof Identifier)
                    && min instanceof Literal
                    && max instanceof Literal) {

                QualifiedColumn column = refMap.get(value);
                context.addPredicate(PredicateRange.fromRange(node, column, (Literal) min, (Literal) max));
            } else {
                context.addPredicate(new PredicateOther(node));
            }

            return null;
        }

        @Override
        protected Void visitLogicalExpression(LogicalExpression node, PredicateAnalysis context) {
            if (LogicalExpression.Operator.AND == node.getOperator()) {
                for (Expression expr : node.getTerms()) {
                    process(expr, context);
                }
            } else {
                context.notSupport("unsupported: logicExpression OR:" + node);
            }
            return null;
        }

        @Override
        protected Void visitIsNotNullPredicate(IsNotNullPredicate node, PredicateAnalysis context) {
            context.addPredicate(new PredicateOther(node));
            return null;
        }

        @Override
        protected Void visitIsNullPredicate(IsNullPredicate node, PredicateAnalysis context) {
            context.addPredicate(new PredicateOther(node));
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, PredicateAnalysis context) {
            context.addPredicate(new PredicateOther(node));
            return null;
        }
    }

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
            ExpressionRewriter rewriter) {

        List<Expression> queryOther = new ArrayList<>();
        String err1 = replaceColumnInCondition(queryOtherList, queryOther, rewriter);
        if (err1 != null) {
            return err1;
        }

        List<Expression> mvOther = new ArrayList<>();
        String err2 = replaceColumnInCondition(mvOtherList, mvOther, rewriter);
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
            List<PredicateOther> other, List<Expression> replaced, ExpressionRewriter rewriter) {
        if (other == null || other.size() == 0) {
            return null;
        }

        for (PredicateOther predicate : other) {
            if (predicate.isAlwaysTrue()) {
                continue;
            }

            Expression before = predicate.getExpr();
            Expression after = rewriter.process(before);
            if (after != null) {
                replaced.add(after);
            } else {
                return "predicate: could not handle other predicate" + before;
            }
        }

        return null;
    }
}
