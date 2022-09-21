package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.predicate.Predicate;
import io.trino.sql.rewritemv.predicate.PredicateAnalysis;
import io.trino.sql.rewritemv.predicate.PredicateEqual;
import io.trino.sql.rewritemv.predicate.PredicateOther;
import io.trino.sql.rewritemv.predicate.PredicateRange;
import io.trino.sql.rewritemv.predicate.PredicateUtil;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.SelectItem;

import java.util.ArrayList;
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

        wherePredicate = RewriteUtils.analyzePredicate(optWhere.get(), specRewriter.getColumnRefMap());
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
        List<Predicate> compensation = new ArrayList<>();
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
        mvSelectableColumnExtend = RewriteUtils.extendSelectableColumnByEc(mvDetail.getSelectableColumn(),
                wherePredicate.getEcList());

        List<Predicate> list2 = new ArrayList<>();
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

        // predicateOther的处理
        List<Expression> list3 = new ArrayList<>();
        String errProcessOther = PredicateUtil.processPredicateOther(wherePredicate.getOtherList(),
                mvDetail.getWherePredicate().getOtherList(),
                list3,
                mvSelectableColumnExtend,
                columnRefMap,
                mvDetail
        );
        if (errProcessOther != null) {
            notFit("where: " + errProcessOther);
            return null;
        }
        conditions.addAll(list3);

        if (conditions == null || conditions.size() == 0) {
            return null;
        } else if (conditions.size() == 1) {
            return conditions.get(0);
        } else {
            return new LogicalExpression(LogicalExpression.Operator.AND, conditions);
        }
    }

    private List<Expression> parseAtomicWhere(List<Predicate> compensation) {
        List<Expression> list = new ArrayList<>(compensation.size() + 3);

        for (Predicate predicate : compensation) {
            if (predicate instanceof PredicateEqual) {
                PredicateEqual pe = (PredicateEqual) predicate;
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
            } else if (predicate instanceof PredicateRange) {
                PredicateRange pr = (PredicateRange) predicate;
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
                PredicateOther other = (PredicateOther) predicate;
                // 需要替换 column
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

    public PredicateAnalysis getWherePredicate() {
        return wherePredicate;
    }

    public Map<QualifiedColumn, SelectItem> getMvSelectableColumnExtend() {
        return mvSelectableColumnExtend;
    }
}
