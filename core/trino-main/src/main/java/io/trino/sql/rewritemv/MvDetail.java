package io.trino.sql.rewritemv;

import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewInfo;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.rewritemv.rewriter.QualifiedSingleColumn;
import io.trino.sql.rewritemv.rewriter.RewriteUtils;
import io.trino.sql.tree.*;

import java.util.List;
import java.util.Map;

public class MvDetail {
    /**
     * the name of this mv
     */
    private final QualifiedObjectName name;
    private final Statement statement;
    private final ViewInfo viewInfo;
    private final Analysis analysis;
    private final Query query;
    private final QuerySpecification querySpecification;

    /**
     * extracted information
     */
    private final Map<Expression, QualifiedSingleColumn> columnRefMap;

    /**
     * select column
     */
    private final Map<QualifiedSingleColumn, SelectItem> selectableColumn;

    /**
     * base table of this mv
     */
    private final List<Table> baseTable;

    private final DereferenceExpression tableNameExpression; // mv的基本表达式, 因为大量用到

    public MvDetail(QualifiedObjectName name, Statement statement, ViewInfo viewInfo, Analysis analysis) {
        this.name = name;
        this.statement = statement;
        this.viewInfo = viewInfo;
        this.analysis = analysis;


        this.query = (Query) analysis.getStatement();
        this.querySpecification = (QuerySpecification) this.query.getQueryBody();
        columnRefMap = RewriteUtils.extractNodeFieldMap(analysis);
        selectableColumn = RewriteUtils.extractSelectSingleField(querySpecification, columnRefMap);

        baseTable = RewriteUtils.extractBaseTable(querySpecification.getFrom().get());

        Identifier catalog = new Identifier(name.getCatalogName());
        Identifier schema = new Identifier(name.getSchemaName());
        Identifier database = new Identifier(name.getObjectName());
        DereferenceExpression catalogSchema = new DereferenceExpression(catalog, schema);
        tableNameExpression = new DereferenceExpression(catalogSchema, database);
    }

    public QualifiedObjectName getName() {
        return name;
    }

    public Statement getStatement() {
        return statement;
    }

    public ViewInfo getViewInfo() {
        return viewInfo;
    }

    public Analysis getAnalysis() {
        return analysis;
    }

    public Query getQuery() {
        return query;
    }

    public QuerySpecification getQuerySpecification() {
        return querySpecification;
    }

    public Map<Expression, QualifiedSingleColumn> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<QualifiedSingleColumn, SelectItem> getSelectableColumn() {
        return selectableColumn;
    }

    public List<Table> getBaseTable() {
        return baseTable;
    }

    public DereferenceExpression getTableNameExpression() {
        return tableNameExpression;
    }
}
