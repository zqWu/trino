package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.predicate.EquivalentClass;
import io.trino.sql.rewritemv.predicate.PredicateAnalysis;
import io.trino.sql.rewritemv.predicate.PredicateUtil;
import io.trino.sql.rewritemv.predicate.visitor.ExpressionRewriter;
import io.trino.sql.rewritemv.predicate.visitor.HavingRewriteVisitor;
import io.trino.sql.rewritemv.predicate.visitor.HavingToWhereRewriteVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.GroupBy;
import io.trino.sql.tree.GroupingElement;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SimpleGroupBy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * groupBy的处理策略:
 * A. 如果 original 没有groupBy 而mv group了, 则无法改写
 * <p>
 * B. original的groupBy字段, 必须是 mv group的子集
 * mv 不支持 having
 * - 支持 having, 无法保证正确性
 */
public class GroupByRewriter {
    private static final Logger LOG = Logger.get(GroupByRewriter.class);
    private final MvDetail mvDetail;
    private final QueryRewriter queryRewriter;
    private final QuerySpecificationRewriter specRewriter;
    private final QuerySpecification origSpec;
    private final QuerySpecification mvSpec;
    private final boolean sameWhere;
    private Optional<GroupBy> resultGroupBy;    // 保留下来的 groupBy
    private Optional<Expression> resultHaving;        // 保留下来的 having
    private Optional<Expression> resultWhere;         // 如果没有 having, 则改为 where条件
    private Set<QualifiedColumn> groupColumns; // 保存

    public GroupByRewriter(QuerySpecificationRewriter specRewriter, MvDetail mvDetail, boolean sameWhere) {
        this.mvDetail = mvDetail;
        this.specRewriter = specRewriter;
        this.queryRewriter = specRewriter.getQueryRewriter();
        this.sameWhere = sameWhere;

        origSpec = queryRewriter.getSpec();
        mvSpec = mvDetail.getMvQuerySpec();

        resultGroupBy = Optional.empty();
        resultHaving = Optional.empty();
        resultWhere = Optional.empty();
    }

    /**
     * 目前暂不支持 having 过滤
     */
    public void process() {
        Optional<GroupBy> origGroupOpt = origSpec.getGroupBy();
        Optional<GroupBy> mvGroupOpt = mvSpec.getGroupBy();

        // 处理了 origGroup=空 的情况
        if (origGroupOpt.isEmpty()) {
            if (mvGroupOpt.isPresent()) {
                notFit("not match : groupBy : original has not groupBy, mv has groupBy");
            }
            return;
        }

        if (mvGroupOpt.isEmpty()) {
            mvNoGroupBy(origGroupOpt.get());
        } else {
            mvHasGroupBy(origGroupOpt.get(), mvGroupOpt.get());
        }
    }

    /**
     * case: mv no groupBy
     */
    private void mvNoGroupBy(GroupBy orig) {
        List<GroupingElement> origList = orig.getGroupingElements();
        List<QualifiedColumn> origGroupByColumn = extractGroupColumn(origList, specRewriter.getColumnRefMap());
        if (origGroupByColumn.size() != origList.size()) {
            notFit("not match : groupBy : contains non column group");
        } else {
            rewriteSimpleGroupBy(orig.isDistinct(), origGroupByColumn);
            processHaving();
        }
    }

    /**
     * compare two groupBy clause
     * - A same: no groupBy need anymore
     * - B mv less: not fit
     * - C mv more and cover: rewrite
     * - D mv more but not cover: not fit
     */
    private void mvHasGroupBy(GroupBy orig, GroupBy mv) {
        if (!Objects.equals(orig.isDistinct(), mv.isDistinct())) {
            notFit("not match : groupBy : groupBy has different isDistinct()");
            return;
        }
        // isDistinct 相同了
        // 把group条件取出来, 进行比较
        List<GroupingElement> origList = orig.getGroupingElements();
        List<GroupingElement> mvList = mv.getGroupingElements();
        if (mvList.size() < origList.size()) {
            // mv less: not fit
            notFit("not match : groupBy : mv groupBy more coarse");
            return;
        }

        // 目前只支持 SimpleGroupBy
        List<QualifiedColumn> origGroupByColumn = extractGroupColumn(origList, specRewriter.getColumnRefMap());
        if (origGroupByColumn.size() != origList.size()) {
            notFit("not match : groupBy : contains non column group");
            return;
        }

        List<QualifiedColumn> mvGroupByColumn = extractGroupColumn(mvList, mvDetail.getMvColumnRefMap());
        if (mvGroupByColumn.size() != mvList.size()) {
            notFit("not match : groupBy : contains non column group");
            return;
        }

        // 确保所有的 orig中的 group 都在包含在mv的group中
        for (QualifiedColumn origCol : origGroupByColumn) {
            if (mvGroupByColumn.contains(origCol)) {
                // 该列 origCol 在 mvGroupByColumn 中
                continue;
            }

            // 不直接包含, 通过ec进行查找
            EquivalentClass ec = specRewriter.getEquivalentClassByColumn(origCol);
            if (ec == null || ec.getColumns().size() == 1) { // size=1表面 只包含 origCol
                notFit("not match : groupBy : original group not contained in mv:" + origCol.toString());
                return;
            }

            // 该ec的一列 在 groupBy中
            boolean mvAlsoGroupByThisEc = mvGroupByColumn.stream().anyMatch(col -> ec.contain(col));
            if (!mvAlsoGroupByThisEc) {
                LOG.debug("not match : groupBy : using ec :" + origCol.toString());
                notFit("groupBy column not in mv:" + origCol);
                return;
            }
        }

        if (origGroupByColumn.size() == mvGroupByColumn.size()) {
            // same groupBy
            LOG.debug("original and mv has same groupBy, no compensation");
            processHaving();
        } else {
            LOG.debug("original and mv has different groupBy, need compensation");
            rewriteSimpleGroupBy(orig.isDistinct(), origGroupByColumn);
            processHaving();
        }
    }

    /**
     * rewrite query groupBy: column replace
     */
    private void rewriteSimpleGroupBy(boolean isDistinct, List<QualifiedColumn> origGroupColumn) {
        List<GroupingElement> groupingElements = new ArrayList<>(origGroupColumn.size());
        groupColumns = new HashSet<>();

        for (QualifiedColumn col : origGroupColumn) {
            DereferenceExpression colInMv = RewriteUtils.findColumnInMv(col, specRewriter.getMvSelectableColumnExtend(), mvDetail.getTableNameExpression());
            if (colInMv == null) {
                notFit("not match : groupBy : mv not have field=" + col);
                return;
            }
            SimpleGroupBy simpleGroupBy = new SimpleGroupBy(Arrays.asList(colInMv));
            groupingElements.add(simpleGroupBy);
            groupColumns.add(new QualifiedColumn(col.getTable(), colInMv.getField().get().getValue()));
        }

        GroupBy groupBy = new GroupBy(isDistinct, groupingElements);
        resultGroupBy = Optional.of(groupBy);
    }

    /**
     * mv的定义中不允许有 having, 因为这回导致正确性问题, 除非 where / groupBy / having 都一致
     * 目前 having条件中不支持以下条件, 因为这些条件放在 where中更合理, 没有看到在 having中使用这些条件的先例
     * - colA = colB
     * - colA > 4
     */
    private void processHaving() {
        Optional<Expression> mvHavingOpt = mvSpec.getHaving();
        Optional<Expression> origHavingOpt = origSpec.getHaving();
        if (origHavingOpt.isEmpty()) {
            if (mvHavingOpt.isPresent()) {
                notFit("having: mv has having clause, query not have");
            }
            return;
        }
        // now original has having clause


        if (mvHavingOpt.isPresent()) {
            if (sameWhere && resultGroupBy.isPresent()) {
                // case: same where/groupBy
                // TODO if having same, then fit, else not fit
                throw new UnsupportedOperationException("TODO if having same, then fit, else not fit");
            } else {
                notFit("having: mv has having clause, but where/groupBy clause not the same");
            }
            return;
        }

        // now original has having, mv no having
        Expression origHaving = origHavingOpt.get();
        PredicateAnalysis origAnalysis = PredicateUtil.analyzePredicate(origHaving, specRewriter.getColumnRefMap());
        if (!origAnalysis.isSupport()) {
            notFit("having not support:" + origAnalysis.getReason());
            return;
        }
        if (origAnalysis.getEcList().size() != 0) {
            notFit("having not support: equal condition in having");
            return;
        }
        if (origAnalysis.getRangeList().size() != 0) {
            notFit("having not support: range condition in having");
            return;
        }

        // 仅处理 PredicateOther
        boolean sameGroupBy = resultGroupBy.isEmpty();
        if (sameGroupBy) {
            sameGroupByRewriteHaving(origAnalysis);
        } else {
            differentGroupByRewriteHaving(origAnalysis);
        }
    }

    /**
     * sameGroupBy 导致没有了 having clause
     * 尝试将这些 original having clause ====> where clause 中
     */
    private void sameGroupByRewriteHaving(PredicateAnalysis origHaving) {
        ExpressionRewriter rewriter = new HavingToWhereRewriteVisitor(
                specRewriter.getMvSelectableColumnExtend(), specRewriter.getColumnRefMap(), mvDetail);

        List<Expression> compensation = new ArrayList<>();
        String errMsg = PredicateUtil.processPredicateOther(
                origHaving.getOtherList(), null, compensation, rewriter);
        if (errMsg != null) {
            notFit("having: " + errMsg);
            return;
        }
        if (compensation.size() == 0) {
            return;
        }
        Expression expr = PredicateUtil.logicAnd(compensation);

        // 将这些 组装到 where中去
        resultWhere = Optional.of(expr);
    }

    private void differentGroupByRewriteHaving(PredicateAnalysis origHaving) {
        boolean isMvHasGroupBy = mvDetail.getMvQuerySpec().getGroupBy().isPresent();
        ExpressionRewriter rewriter = new HavingRewriteVisitor(
                specRewriter.getMvSelectableColumnExtend(), specRewriter.getColumnRefMap(), mvDetail, isMvHasGroupBy);

        List<Expression> compensation = new ArrayList<>();
        String errMsg = PredicateUtil.processPredicateOther(
                origHaving.getOtherList(), null, compensation, rewriter);

        if (errMsg != null) {
            notFit("having: " + errMsg);
            return;
        }
        if (compensation.size() == 0) {
            return;
        }
        Expression expr = PredicateUtil.logicAnd(compensation);

        // 将这些 组装到 where中去
        resultHaving = Optional.of(expr);
    }

    /**
     * 从 groupBy中, 提取 groupBy 使用的col字段
     */
    private List<QualifiedColumn> extractGroupColumn(List<GroupingElement> groupingElements, Map<Expression, QualifiedColumn> refMap) {
        List<QualifiedColumn> resultList = new ArrayList<>(groupingElements.size());
        for (GroupingElement element : groupingElements) {
            if (!(element instanceof SimpleGroupBy)) {
                LOG.debug("not match : groupBy : only support SimpleGroupBy" + element);
                continue;
            }

            List<Expression> expressions = element.getExpressions();
            if (expressions.size() != 1) {
                LOG.debug("not match : groupBy : only support 1 column expression in group" + element);
                continue;
            }

            Expression expr = expressions.get(0);
            QualifiedColumn qualifiedColumn = refMap.get(expr);
            if (qualifiedColumn == null) {
                LOG.debug("not match : groupBy : column's expression cannot be found:" + expr);
                continue;
            }
            resultList.add(qualifiedColumn);
        }
        return resultList;
    }

    public boolean isMvFit() {
        return queryRewriter.isMvFit();
    }

    public void notFit(String reason) {
        queryRewriter.notFit(reason);
    }

    // ======== get

    public Set<QualifiedColumn> getGroupColumns() {
        return groupColumns;
    }

    public Optional<GroupBy> getResultGroupBy() {
        return resultGroupBy;
    }

    public Optional<Expression> getResultHaving() {
        return resultHaving;
    }

    public Optional<Expression> getResultWhere() {
        return resultWhere;
    }
}
