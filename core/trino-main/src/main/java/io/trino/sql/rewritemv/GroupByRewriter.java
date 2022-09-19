package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.where.EquivalentClass;
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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * groupBy的处理策略:
 * A. 如果 original 没有groupBy 而mv group了, 则无法改写
 * <p>
 * B. original的groupBy字段, 必须是 mv group的子集
 * - 如果逻辑上完全相等, 则 groupBy 可以去掉, 把 having条件改成where 条件
 * - 如果是真子集, 则保留 groupBy, 并做必要 字段名修改
 */
public class GroupByRewriter {
    private static final Logger LOG = Logger.get(GroupByRewriter.class);
    private final MvDetail mvDetail;
    private final QueryRewriter queryRewriter;
    private final QuerySpecificationRewriter specRewriter;
    private final QuerySpecification originalSpec;
    private final QuerySpecification mvSpec;
    private Optional<GroupBy> resultGroupBy;    // 保留下来的 groupBy
    private Optional<Expression> having;        // 保留下来的 having
    private Optional<Expression> where;         // 如果没有 having, 则改为 where条件
    private Set<QualifiedColumn> groupColumns; // 保存

    public GroupByRewriter(QuerySpecificationRewriter specRewriter, MvDetail mvDetail) {
        this.mvDetail = mvDetail;
        this.specRewriter = specRewriter;
        this.queryRewriter = specRewriter.getQueryRewriter();

        originalSpec = queryRewriter.getSpec();
        mvSpec = mvDetail.getQuerySpecification();

        resultGroupBy = Optional.empty();
        having = Optional.empty();
        where = Optional.empty();
    }

    /**
     * 目前暂不支持 having 过滤
     */
    public void process() {
        Optional<GroupBy> origGroupOpt = originalSpec.getGroupBy();
        Optional<GroupBy> mvGroupOpt = mvSpec.getGroupBy();

        // 处理了 origGroup=空 的情况
        if (origGroupOpt.isEmpty()) {
            if (mvGroupOpt.isPresent()) {
                notFit("groupBy rewrite: original has not groupBy, whereas mv has groupBy");
            }
            return;
        }

        if (mvGroupOpt.isEmpty()) {
            // group: orig有, mv无 TODO
            return;
        }

        // origGroupOpt 非空
        compareAndRewriteGroup(origGroupOpt.get(), mvGroupOpt.get());
    }

    private void compareAndRewriteGroup(GroupBy orig, GroupBy mv) {
        if (!Objects.equals(orig.isDistinct(), mv.isDistinct())) {
            notFit("groupBy rewrite: groupBy has different isDistinct()");
            return;
        }
        // isDistinct 相同了
        // 把group条件取出来, 进行比较
        List<GroupingElement> origList = orig.getGroupingElements();
        List<GroupingElement> mvList = mv.getGroupingElements();
        if (mvList.size() < origList.size()) {
            notFit("groupBy rewrite: mv groupBy more coarse");
            return;
        }

        // 目前只支持 SimpleGroupBy
        List<QualifiedColumn> origGroupByColumn = extractGroupColumn(origList);
        if (origGroupByColumn == null) {
            return;
        }

        List<QualifiedColumn> mvGroupByColumn = extractGroupColumn(mvList);
        if (mvGroupByColumn == null) {
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
                notFit("groupBy rewrite: original group not contained in mv:" + origCol.toString());
                return;
            }

            // 该ec的一列 在 groupBy中
            boolean mvAlsoGroupByThisEc = mvGroupByColumn.stream().anyMatch(col -> ec.contain(col));
            if (!mvAlsoGroupByThisEc) {
                LOG.debug("groupBy rewrite: using ec :" + origCol.toString());
            }
        }

        if (origGroupByColumn.size() == mvGroupByColumn.size()) {
            LOG.debug("original and mv has same groupBy, no compensation");
            processHaving();
        } else {
            LOG.debug("original and mv has different groupBy, need compensation");
            rewriteSimpleGroupBy(orig.isDistinct(), origGroupByColumn);
            processHaving();
        }
    }

    private void rewriteSimpleGroupBy(boolean isDistinct, List<QualifiedColumn> origGroupColumn) {
        List<GroupingElement> groupingElements = new ArrayList<>(origGroupColumn.size());
        groupColumns = new HashSet<>();

        for (QualifiedColumn col : origGroupColumn) {
            DereferenceExpression colInMv = RewriteUtils.findColumnInMv(col, specRewriter.getMvSelectableColumnExtend(), mvDetail.getTableNameExpression());
            if (colInMv == null) {
                notFit("groupBy rewrite: mv not have field=" + col);
                return;
            }
            SimpleGroupBy simpleGroupBy = new SimpleGroupBy(Arrays.asList(colInMv));
            groupingElements.add(simpleGroupBy);
            groupColumns.add(new QualifiedColumn(col.getTable(), colInMv.getField().get().getValue()));
        }

        GroupBy groupBy = new GroupBy(isDistinct, groupingElements);
        resultGroupBy = Optional.of(groupBy);
    }

    private void processHaving() {
        // TODO 处理 having

    }

    private List<QualifiedColumn> extractGroupColumn(List<GroupingElement> groupingElements) {
        List<QualifiedColumn> resultList = new ArrayList<>(groupingElements.size());
        for (GroupingElement element : groupingElements) {
            if (!(element instanceof SimpleGroupBy)) {
                notFit("groupBy rewrite: only support SimpleGroupBy" + element);
                return null;
            }

            List<Expression> expressions = element.getExpressions();
            if (expressions.size() != 1) {
                notFit("groupBy rewrite: only support 1 column expression in group" + element);
                return null;
            }

            Expression expr = expressions.get(0);
            QualifiedColumn qualifiedColumn = specRewriter.getColumnRefMap().get(expr);
            if (qualifiedColumn == null) {
                throw new RuntimeException("groupBy rewrite: column's expression cannot be found:" + expr);
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

    public Optional<Expression> getHaving() {
        return having;
    }

    public Optional<Expression> getWhere() {
        return where;
    }
}
