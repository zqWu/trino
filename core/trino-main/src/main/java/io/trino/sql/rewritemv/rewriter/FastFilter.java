package io.trino.sql.rewritemv.rewriter;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.tree.*;

import java.util.*;

/**
 * 快速 mv 比对 original analysis 和 mvAnalysis
 * 1. mv 包含所需要的 base table
 * 2. mv 包含所需要的 row
 * <p>
 * 注: 即使通过了 FastFilter, 后续匹配也可能失败, 这个仅是一个快速筛选
 */
class FastFilter {
    private static final Logger LOG = Logger.get(FastFilter.class);
    private final Analysis originalAnalysis;
    private final Analysis mvAnalysis;
    private final Query originalQuery;
    private final Query mvQuery;

    public FastFilter(Analysis originalAnalysis, Analysis mvAnalysis) {
        this.originalAnalysis = originalAnalysis;
        this.mvAnalysis = mvAnalysis;

        originalQuery = (Query) originalAnalysis.getStatement();
        mvQuery = (Query) mvAnalysis.getStatement();
    }

    public boolean process() {
        if (!checkTable()) {
            LOG.debug("check table 不通过");
            return false;
        }

        if (!checkColumn()) {
            LOG.debug("check Column 不通过");
            return false;
        }

        if (!checkWhere()) {
            LOG.debug("check Where 不通过");
            return false;
        }
        return true;
    }

    // mv必须包含需要的 base table
    private boolean checkTable() {
        Set<QualifiedObjectName> origTables = originalAnalysis.getTableName();
        Set<QualifiedObjectName> mvTables = mvAnalysis.getTableName();
        if (!mvTables.containsAll(origTables)) {
            LOG.debug("FastFilter: not pass, table not covering");
            return false;
        }
        return true;
    }

    // mv必须包含需要的 column
    private boolean checkColumn() {
        Map<NodeRef<Expression>, ResolvedField> mvColumnReferenceFields = mvAnalysis.getColumnReferenceFields();
        Set<QualifiedSingleColumn> mvColumn = new HashSet<>(mvColumnReferenceFields.size());
        for (Map.Entry<NodeRef<Expression>, ResolvedField> entry : mvColumnReferenceFields.entrySet()) {
            Field field = entry.getValue().getField();
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                QualifiedObjectName table = field.getOriginTable().get();
                String columnName = field.getOriginColumnName().get();
                mvColumn.add(new QualifiedSingleColumn(table, columnName));
            } else {
                // 这个是个 计算字段, 如 select t.a, t.b, count(1), 这里 count(1)就是计算字段
                LOG.debug("1 ignore non table:column field [%s]", field.getName().get());
            }
        }

        Map<NodeRef<Expression>, ResolvedField> origColumnReferenceFields = originalAnalysis.getColumnReferenceFields();
        for (Map.Entry<NodeRef<Expression>, ResolvedField> entry : origColumnReferenceFields.entrySet()) {
            Field field = entry.getValue().getField();
            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                QualifiedObjectName table = field.getOriginTable().get();
                String columnName = field.getOriginColumnName().get();
                if (!mvColumn.contains(new QualifiedSingleColumn(table, columnName))) {
                    LOG.debug("FastFilter: not pass, row not covering");
                    return false;
                }
            } else {
                // 这个是个 计算字段, 如 select t.a, t.b, count(1), 这里 count(1)就是计算字段
                LOG.debug("2 ignore non table:column field [%s]", field.getName().get());
            }
        }
        return true;
    }

    private boolean checkWhere() {
        QuerySpecification orig = (QuerySpecification) originalQuery.getQueryBody();
        QuerySpecification mv = (QuerySpecification) mvQuery.getQueryBody();

        Optional<Expression> o1 = orig.getWhere();
        Optional<Expression> o2 = mv.getWhere();

        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }

        if (o1.isEmpty() || o2.isEmpty()) {
            return false;
        }

        Expression origWhereExpr = o1.get();
        Expression mvWhereExpr = o2.get();

        // simple expression
        if (Objects.equals(origWhereExpr, mvWhereExpr)) {
            return true;
        }
        if (!(origWhereExpr.getClass().equals(mvWhereExpr.getClass()))) {
            // if not same class
            return false;
        }

        // 复杂 where 比较, 目前支持方式:
        // 1. 都是 and 方式
        // 2. original 中的 条件更多
        //    original: where a=1 and b=2 and kk.c=3
        //    mv      : where a=1 and b=2
        // 3. mv中缺少的 where条件(kk.c=3), 在其结果集(select字段)中 存在
        if ((origWhereExpr instanceof LogicalExpression)) {
            LogicalExpression l1 = (LogicalExpression) origWhereExpr;
            LogicalExpression l2 = (LogicalExpression) mvWhereExpr;
            if (l1.getOperator() != l2.getOperator()) {
                return false;
            }
            if (l1.getOperator() != LogicalExpression.Operator.AND) {
                return false;
            }
            // TODO 更细致的 where条件比较
        }

        return true;
    }
}
