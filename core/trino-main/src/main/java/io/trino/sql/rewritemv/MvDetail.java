package io.trino.sql.rewritemv;

import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewInfo;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.rewritemv.where.EquivalentClass;
import io.trino.sql.rewritemv.where.WhereAnalysis;
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
import java.util.Set;
import java.util.stream.Collectors;

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
    private final Map<Expression, QualifiedColumn> columnRefMap;
    private final Map<QualifiedColumn, SelectItem> selectableColumn;
    private final List<Table> baseTable;
    private final DereferenceExpression tableNameExpression; // mv的基本表达式, 因为大量用到
    private final WhereAnalysis whereAnalysis;

    public MvDetail(QualifiedObjectName name, Statement statement, ViewInfo viewInfo, Analysis analysis) {
        this.name = name;
        this.statement = statement;
        this.viewInfo = viewInfo;
        this.analysis = analysis;


        this.query = (Query) analysis.getStatement();
        this.querySpecification = (QuerySpecification) this.query.getQueryBody();
        columnRefMap = RewriteUtils.extractColumnReferenceMap(analysis);
        selectableColumn = RewriteUtils.extractSelectSingleField(querySpecification, columnRefMap);

        if (querySpecification.getWhere().isPresent()) {
            this.whereAnalysis = RewriteUtils.analyzeWhere(querySpecification.getWhere().get(), columnRefMap);
        } else {
            this.whereAnalysis = WhereAnalysis.EMPTY_WHERE;
        }

        //
        baseTable = RewriteUtils.extractBaseTable(querySpecification.getFrom().get());

        //
        Identifier catalog = new Identifier(name.getCatalogName());
        Identifier schema = new Identifier(name.getSchemaName());
        Identifier database = new Identifier(name.getObjectName());
        DereferenceExpression catalogSchema = new DereferenceExpression(catalog, schema);
        tableNameExpression = new DereferenceExpression(catalogSchema, database);
    }

    // ======== get

    public WhereAnalysis getWhereAnalysis() {
        return whereAnalysis;
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

    public Map<Expression, QualifiedColumn> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<QualifiedColumn, SelectItem> getSelectableColumn() {
        return selectableColumn;
    }

    public SelectItem findColumn(EquivalentClass ec, Set<QualifiedColumn> mustIn) {
        Set<QualifiedColumn> columns = ec.getColumns();
        if (columns == null || columns.size() == 0) {
            return null;
        }
        if (mustIn != null && mustIn.size() > 0) {
            columns = columns.stream().filter(mustIn::contains).collect(Collectors.toSet());
        }

        for (QualifiedColumn col : columns) {
            SelectItem selectItem = selectableColumn.get(col);
            if (selectItem != null) {
                return selectItem;
            }
        }
        return null;
    }

    public List<Table> getBaseTable() {
        return baseTable;
    }

    public DereferenceExpression getTableNameExpression() {
        return tableNameExpression;
    }
}
