package io.trino.sql.rewritemv;

import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewInfo;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.rewritemv.predicate.PredicateAnalysis;
import io.trino.sql.rewritemv.predicate.PredicateUtil;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.Statement;
import io.trino.sql.tree.Table;

import java.util.List;
import java.util.Map;

public class MvDetail {
    private final QualifiedObjectName mvName; // mv name
    private final Statement mvStatement;
    private final ViewInfo viewInfo;
    private final Analysis mvAnalysis;
    private final Query mvQuery;
    private final QuerySpecification mvQuerySpec;
    private final Map<Expression, QualifiedColumn> mvColumnRefMap;
    private final Map<QualifiedColumn, SelectItem> mvSelectableColumn;
    private final List<Table> mvBaseTable;
    private final DereferenceExpression tableNameExpression; // mv的基本表达式, 因为大量用到
    private final PredicateAnalysis mvWherePredicate;

    public MvDetail(QualifiedObjectName mvName, Statement statement, ViewInfo viewInfo, Analysis analysis) {
        this.mvName = mvName;
        this.mvStatement = statement;
        this.viewInfo = viewInfo;
        this.mvAnalysis = analysis;


        // ======== 一些简单的变量提取出来, 便于使用
        this.mvQuery = (Query) analysis.getStatement();
        this.mvQuerySpec = (QuerySpecification) this.mvQuery.getQueryBody();
        mvBaseTable = RewriteUtils.extractBaseTable(mvQuerySpec.getFrom().get());
        DereferenceExpression catalogAndSchema = new DereferenceExpression(
                new Identifier(mvName.getCatalogName()), new Identifier(mvName.getSchemaName()));
        tableNameExpression = new DereferenceExpression(catalogAndSchema, new Identifier(mvName.getObjectName()));

        // ======== 提取 column reference map Map<Expression, QualifiedColumn>
        mvColumnRefMap = RewriteUtils.extractColumnReferenceMap(analysis);

        // ======== 分析 where
        if (mvQuerySpec.getWhere().isPresent()) {
            this.mvWherePredicate = PredicateUtil.analyzePredicate(mvQuerySpec.getWhere().get(), mvColumnRefMap);
        } else {
            this.mvWherePredicate = PredicateAnalysis.EMPTY_PREDICATE;
        }
        // ======== 提取 selectableColumn, Map<QualifiedColumn, SelectItem>
        mvSelectableColumn = RewriteUtils.extractSelectSingleField(mvQuerySpec, mvColumnRefMap, mvWherePredicate);
    }

    // ======== get

    public PredicateAnalysis getMvWherePredicate() {
        return mvWherePredicate;
    }

    public QualifiedObjectName getMvName() {
        return mvName;
    }

    public Statement getMvStatement() {
        return mvStatement;
    }

    public ViewInfo getViewInfo() {
        return viewInfo;
    }

    public Analysis getMvAnalysis() {
        return mvAnalysis;
    }

    public Query getMvQuery() {
        return mvQuery;
    }

    public QuerySpecification getMvQuerySpec() {
        return mvQuerySpec;
    }

    public Map<Expression, QualifiedColumn> getMvColumnRefMap() {
        return mvColumnRefMap;
    }

    public Map<QualifiedColumn, SelectItem> getMvSelectableColumn() {
        return mvSelectableColumn;
    }

    public List<Table> getMvBaseTable() {
        return mvBaseTable;
    }

    public DereferenceExpression getTableNameExpression() {
        return tableNameExpression;
    }
}
