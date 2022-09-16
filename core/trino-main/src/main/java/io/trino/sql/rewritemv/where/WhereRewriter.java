package io.trino.sql.rewritemv.where;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.rewritemv.QueryRewriter;
import io.trino.sql.rewritemv.QuerySpecificationRewriter;
import io.trino.sql.rewritemv.RewriteUtils;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LogicalExpression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;

/**
 * rewrite where条件
 */
public class WhereRewriter {
    private static final Logger LOG = Logger.get(WhereRewriter.class);
    private final QueryRewriter queryRewriter;
    private final QuerySpecificationRewriter specRewriter;
    private final MvDetail mvDetail;
    private WhereAnalysis whereAnalysis;

    public WhereRewriter(QuerySpecificationRewriter specRewriter, MvDetail mvDetail) {
        this.specRewriter = specRewriter;
        this.queryRewriter = specRewriter.getQueryRewriter();
        this.mvDetail = mvDetail;
    }

    /**
     * 返回改写后的 where
     */
    public Expression process() {
        Optional<Expression> optWhere = queryRewriter.getSpec().getWhere();
        Optional<Expression> mvWhere = mvDetail.getQuerySpecification().getWhere();
        WhereAnalysis mvWhereAnalysis = mvDetail.getWhereAnalysis();

        // case: query where=null
        if (optWhere.isEmpty()) {
            if (mvWhere.isPresent()) {
                if (mvWhereAnalysis.hasEffectivePredict()) {
                    // mv包含有效的 where过滤, 则无法匹配
                    notFit("where(original) = empty,  where(mv) != empty");
                }
            }
            return null;
        }

        if (!mvWhereAnalysis.isSupport()) {
            notFit("where(mv) not support, reason=" + mvWhereAnalysis.getReason());
            return null;
        }

        whereAnalysis = RewriteUtils.analyzeWhere(optWhere.get(), specRewriter.getColumnRefMap());
        if (!whereAnalysis.isSupport()) {
            notFit("where(original) not support, reason=" + whereAnalysis.getReason());
        }
        specRewriter.setEcList(whereAnalysis.getEcList());

        // TODO
        // 根据 whereAnalysis 和 mvDetail.mvAnalysis 处理 where
        // 1. 所有 mvWhereAnalysis中的条件, 在 originalWhereAnalysis 都有
        // 2. originalWhereAnalysis 多下来的条件, 需要适当的改写
        // where a=b vs where a=1 and b=1, 虽然后者能够推到前者, 但是目前不做这方面工作

        return compareAndRewriteWhere();
    }

    private Expression compareAndRewriteWhere() {
        List<AtomicWhere> compensation = processPredictEqual();

        List<AtomicWhere> list2 = processPredictRange();
        compensation.addAll(list2);

        // 3. 剩下的 predict 处理


        // 整理 compensation
        List<Expression> conditions = parseAtomicWhere(compensation);
        if (conditions == null || conditions.size() == 0) {
            return null;
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            return new LogicalExpression(LogicalExpression.Operator.AND, conditions);
        }
    }

    // predictEqual处理
    private List<AtomicWhere> processPredictEqual() {
        List<AtomicWhere> compensation = new ArrayList<>(); // 存放比较后的 补偿条件
        WhereAnalysis mv = mvDetail.getWhereAnalysis();
        // 1. 检测 mv中的条件, orig都有

        // 1.1 EquivalentClass contain, eg,
        // mv   eg=[ (colA, colB),       (colC, colD),       (colM, comN)]
        // orig eg=[ (colA, colB, colE), (colC, colD, colY), (colM)      ]
        List<EquivalentClass> origEcList = whereAnalysis.getEcList();
        Map<EquivalentClass, List<EquivalentClass>> map = new HashMap<>();

        for (EquivalentClass mvEc : mv.getEcList()) {
            if (mvEc.getColumns().size() < 2) {
                continue;
            }

            QualifiedColumn oneColumnInEc = mvEc.getColumns().stream().findAny().get();

            Optional<EquivalentClass> origEcOpt = origEcList.stream().filter(x -> x.contain(oneColumnInEc)).findAny();
            if (origEcOpt.isEmpty()) {
                notFit("where in mv[%s] not exists in query");
                return null;
            }
            EquivalentClass origEc = origEcOpt.get();

            if (!origEc.getColumns().containsAll(mvEc.getColumns())) {
                // eg (colM, colN) --- (colM)
                notFit("where in query not cover where in mv");
                return null;
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
                    compensation.add(new PredictEqual(null, col0, list.get(i)));
                }
            }
        }

        return compensation;
    }

    // range 条件处理
    private List<AtomicWhere> processPredictRange() {
        List<AtomicWhere> compensation = new ArrayList<>(); // 存放比较后的 补偿条件
        WhereAnalysis mv = mvDetail.getWhereAnalysis();

        List<PredictRange> mvPrList = mv.getPrList();
        List<PredictRange> origPrList = whereAnalysis.getPrList();
        for (EquivalentClass origEc : whereAnalysis.getEcList()) {
            // 在这个 origEc上的 所有 range 进行合并
            List<PredictRange> origRangeOnThisEc = origPrList.stream()
                    .filter(pr -> origEc.getColumns().contains(pr.getLeft()))
                    .collect(Collectors.toList());

            List<PredictRange> mvRangeOnThisEc = mvPrList.stream()
                    .filter(pr -> origEc.getColumns().contains(pr.getLeft()))
                    .collect(Collectors.toList());

            if (origRangeOnThisEc.size() == 0) {
                if (mvRangeOnThisEc.size() == 0) {
                    continue;
                } else {
                    notFit("mv condition not cover by original");
                    return compensation;
                }
            }

            QualifiedColumn column = origEc.getColumns().stream().findAny().get();
            PredictRange shouldSmallRange = PredictRange.unbound(column, origEc);
            for (int i = 0; i < origRangeOnThisEc.size(); i++) {
                shouldSmallRange = shouldSmallRange.intersection(origRangeOnThisEc.get(i));
            }

            PredictRange shouldLargeRange = PredictRange.unbound(column, origEc);
            for (int i = 0; i < mvRangeOnThisEc.size(); i++) {
                shouldLargeRange = shouldLargeRange.intersection(mvRangeOnThisEc.get(i));
            }
            if (!shouldLargeRange.coverOtherValue(shouldSmallRange)) {
                notFit("mv range not cover original");
                return compensation;
            }
            PredictRange comp = shouldSmallRange.baseOn(shouldLargeRange);
            if (comp.getEqual() != PredictRange.PredictRangeBound.UNBOUND
                    || comp.getLower() != PredictRange.PredictRangeBound.UNBOUND
                    || comp.getUpper() != PredictRange.PredictRangeBound.UNBOUND
            ) {
                compensation.add(comp);
            }
        }

        return compensation;
    }

    private List<Expression> parseAtomicWhere(List<AtomicWhere> compensation) {
        List<Expression> list = new ArrayList<>(compensation.size() + 3);

        for (AtomicWhere atomicWhere : compensation) {
            if (atomicWhere instanceof PredictEqual) {
                PredictEqual pe = (PredictEqual) atomicWhere;
                QualifiedColumn left = pe.getLeft();
                QualifiedColumn right = pe.getRight();
                DereferenceExpression mvLeft = RewriteUtils.findColumnInMv(left, mvDetail);
                DereferenceExpression mvRight = RewriteUtils.findColumnInMv(right, mvDetail);
                if (mvLeft == null) {
                    notFit("cannot find column in mv:" + left);
                    return list;
                }
                if (mvRight == null) {
                    notFit("cannot find column in mv:" + right);
                    return list;
                }
                list.add(new ComparisonExpression(EQUAL, mvLeft, mvRight));
            } else if (atomicWhere instanceof PredictRange) {
                PredictRange pr = (PredictRange) atomicWhere;
                DereferenceExpression columnInMv = RewriteUtils.findColumnInMv(pr.getEc(), mvDetail);
                if (pr.getEqual() != PredictRange.PredictRangeBound.UNBOUND) {
                    list.add(new ComparisonExpression(EQUAL, columnInMv, pr.getEqual().getValue()));
                }
                if (pr.getLower() != PredictRange.PredictRangeBound.UNBOUND) {
                    list.add(new ComparisonExpression(pr.getLower().getOp(), columnInMv, pr.getLower().getValue()));
                }
                if (pr.getUpper() != PredictRange.PredictRangeBound.UNBOUND) {
                    list.add(new ComparisonExpression(pr.getUpper().getOp(), columnInMv, pr.getUpper().getValue()));
                }
            } else {
                // PredicateOther
                LOG.debug("TODO PredicateOther 条件的处理");
            }
        }

        return list;
    }

    private boolean isMvFit() {
        return queryRewriter.isMvFit();
    }

    public void notFit(String reason) {
        queryRewriter.notFit(reason);
    }

}
