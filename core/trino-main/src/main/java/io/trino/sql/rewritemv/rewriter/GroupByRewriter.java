package io.trino.sql.rewritemv.rewriter;

import io.airlift.log.Logger;
import io.trino.sql.tree.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * groupBy的处理策略:
 * A. 如果 original 没有groupBy 而mv group了, 则无法改写
 * <p>
 * B. original的groupBy字段, 必须是 mv group的子集
 * - 如果逻辑上完全相等, 则 groupBy 可以去掉, 把 having条件改成where 条件
 * - 如果是真子集, 则保留 groupBy, 并做必要 字段名修改
 */
public class GroupByRewriter {
    private static final Logger LOG = Logger.get(WhereRewriter.class);
    private final QuerySpecRewriter querySpecRewriter;
    private final QuerySpecification originalSpec;
    private final QuerySpecification mvSpec;
    private Optional<GroupBy> resultGroupBy; // 保留下来的 groupBy
    private Optional<Expression> having;     // 保留下来的 having
    private Optional<Expression> where;      // 如果没有 having, 则改为 where条件

    public GroupByRewriter(QuerySpecRewriter querySpecRewriter) {
        this.querySpecRewriter = querySpecRewriter;
        originalSpec = querySpecRewriter.getOriginalSpec();
        mvSpec = querySpecRewriter.getMvSpec();

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
                notFit("groupBy rewrite: original 没有groupBy, 而 mv groupBy了, 无法改写");
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
            notFit("groupBy rewrite: groupBy isDistinct()不同, 无法改写");
            return;
        }
        // isDistinct 相同了
        // 把group条件取出来, 进行比较
        List<GroupingElement> origList = orig.getGroupingElements();
        List<GroupingElement> mvList = mv.getGroupingElements();
        if (mvList.size() < origList.size()) {
            notFit("groupBy rewrite: mv的groupBy力度更粗粒, 无法改写");
            return;
        }

        // 目前只支持 SimpleGroupBy
        List<QualifiedSingleColumn> origListColumn = extractGroupColumn(origList);
        if (origListColumn == null) {
            return;
        }

        List<QualifiedSingleColumn> mvListColumn = extractGroupColumn(mvList);
        if (mvListColumn == null) {
            return;
        }

        // 确保所有的 orig中的 group 都在包含在mv的group中
        for (QualifiedSingleColumn origCol : origListColumn) {
            if (!mvListColumn.contains(origCol)) {
                notFit("groupBy rewrite: original group not contained in mv:" + origCol.toString());
                return;
            }
        }

        if (origListColumn.size() == mvListColumn.size()) {
            LOG.debug("2个group一样, 不需要补充group");
            processHaving();
        } else {
            LOG.debug("2个group不一样, 需要把 original的group 都改写一遍");
            rewriteSimpleGroupBy(orig.isDistinct(), origListColumn);
            processHaving();
        }
    }

    private void rewriteSimpleGroupBy(boolean isDistinct, List<QualifiedSingleColumn> cols) {
        List<GroupingElement> groupingElements = new ArrayList<>(cols.size());

        for (QualifiedSingleColumn col : cols) {
            DereferenceExpression colInMv = querySpecRewriter.correspondColumnInMv(col);
            if (colInMv == null) {
                notFit("groupBy rewrite: mv not have field=" + col);
                return;
            }
            SimpleGroupBy simpleGroupBy = new SimpleGroupBy(Arrays.asList(colInMv));
            groupingElements.add(simpleGroupBy);
        }

        GroupBy groupBy = new GroupBy(isDistinct, groupingElements);
        resultGroupBy = Optional.of(groupBy);
    }

    private void processHaving() {
        // TODO 处理 having

    }

    private List<QualifiedSingleColumn> extractGroupColumn(List<GroupingElement> groupingElements) {
        List<QualifiedSingleColumn> resultList = new ArrayList<>(groupingElements.size());
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
            QualifiedSingleColumn qualifiedSingleColumn = querySpecRewriter.getOriginalColumnRefMap().get(expr);
            if (qualifiedSingleColumn == null) {
                throw new RuntimeException("groupBy 字段的 expression找不到相关信息:" + expr);
            }
            resultList.add(qualifiedSingleColumn);
        }
        return resultList;
    }

    public boolean isMvFit() {
        return querySpecRewriter.isMvFit();
    }

    public void notFit(String reason) {
        querySpecRewriter.notFit(reason);
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
