package io.trino.sql.rewritemv.rewriter;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.tree.*;

import java.util.*;

class QuerySpecRewriter {
    private static final Logger LOG = Logger.get(QuerySpecRewriter.class);
    private final MaterializedViewRewriter materializedViewRewriter;
    private final QuerySpecification originalSpec;
    private final QuerySpecification mvSpec;
    private final Map<Expression, QualifiedSingleColumn> originalColumnRefMap;
    private final Map<Expression, QualifiedSingleColumn> mvColumnRefMap;
    private final Map<QualifiedSingleColumn, SelectItem> mvSelectableColumn;

    private final DereferenceExpression mvExpression; // mv的基本表达式, 因为大量用到

    public QuerySpecRewriter(MaterializedViewRewriter materializedViewRewriter) {
        this.materializedViewRewriter = materializedViewRewriter;

        Query b1 = (Query) materializedViewRewriter.getOriginalAnalysis().getStatement();
        originalSpec = (QuerySpecification) b1.getQueryBody();

        Query b2 = (Query) materializedViewRewriter.getMvAnalysis().getStatement();
        mvSpec = (QuerySpecification) b2.getQueryBody();


        originalColumnRefMap = extractNodeFieldMap(materializedViewRewriter.getOriginalAnalysis());
        mvColumnRefMap = extractNodeFieldMap(materializedViewRewriter.getMvAnalysis());
        mvSelectableColumn = extractSelectSingleField(mvSpec, mvColumnRefMap);

        // mvExpression = [catalog, schema, object] 这样的一个嵌套名称
        QualifiedObjectName mvName = getMaterializedViewRewriter().getMvName();
        Identifier catalog = new Identifier(mvName.getCatalogName());
        Identifier schema = new Identifier(mvName.getSchemaName());
        Identifier database = new Identifier(mvName.getObjectName());
        DereferenceExpression catalogSchema = new DereferenceExpression(catalog, schema);
        mvExpression = new DereferenceExpression(catalogSchema, database);

    }

    /*
        sql 的执行顺序
        from join on
        where
        groupBy + aggregate function
        having
        windows
        select
        distinct
        union / intersect / except
        orderBy
        offset
        limit
     */
    public QuerySpecification process() {
        // 逐个计算
        Optional<Relation> relation = processFrom();
        Optional<Expression> where = processWhere();
        GroupByRewriter groupByRewriter = new GroupByRewriter(this);
        groupByRewriter.process();
        Select select = processSelect(originalSpec, mvSpec);

        QuerySpecification spec = originalSpec;
        if (isMvFit()) {
            // TODO
            spec = new QuerySpecification(
                    select,
                    relation,
                    where, // TODO 需要合并 groupByWriter中的 where
                    groupByRewriter.getResultGroupBy(),
                    groupByRewriter.getHaving(),
                    originalSpec.getWindows(),
                    originalSpec.getOrderBy(),
                    originalSpec.getOffset(),
                    originalSpec.getLimit());
        }
        return spec;
    }

    private Select processSelect(QuerySpecification originalSpec, QuerySpecification mvSpec) {
        Select origSelect = originalSpec.getSelect();
        Select mvSelect = mvSpec.getSelect();

        if (!Objects.equals(origSelect.isDistinct(), mvSelect.isDistinct())) {
            notFit("select 无法匹配, 一个有 distinct, 一个没有");
            return origSelect;
        }

        if (Objects.equals(origSelect, mvSelect)) {
            return origSelect;
        }

        List<SelectItem> rewrite = new ArrayList<>();
        for (SelectItem origSelectItem : origSelect.getSelectItems()) {
            if (origSelectItem instanceof SingleColumn) {
                // orig: select t1.a, ...
                // mv:   select t1.a as t1a, ....
                // 修改:  select mv.t1a as a .... from mv
                SingleColumn origColumn = (SingleColumn) origSelectItem;

                Expression expression = origColumn.getExpression();
                QualifiedSingleColumn qColumn = originalColumnRefMap.get(expression);
                if (qColumn == null) {
                    // 不涉及字段, 直接加入. 如 select count(1), 这里的 count(1)
                    rewrite.add(origSelectItem);
                } else {
                    SingleColumn mvColumn = (SingleColumn) mvSelectableColumn.get(qColumn);
                    if (mvColumn == null) {
                        notFit("原sql中的 select 字段无法在 mv中获取");
                        return origSelect;
                    }

                    Identifier mvColumnLast = getNameLastPart(mvColumn);
                    Identifier origColumnLast = getNameLastPart(origColumn);
                    // 获取mv中该字段的名称
                    Expression eee = new DereferenceExpression(mvExpression, mvColumnLast); // 这个用mv字段名, ru iceberg.tpch_tiny.orders
                    SingleColumn a1 = new SingleColumn(eee, origColumnLast);
                    rewrite.add(a1);
                }
            } else if (origSelectItem instanceof AllColumns) {
                notFit("原select子句中有 AllColumns, 暂不支持");
            }
        }

        Select s = new Select(origSelect.isDistinct(), rewrite);
        return s;
    }

    private Optional<Expression> processWhere() {
        return new WhereRewriter(this).process();
    }

    // currently only support simple table
    private Optional<Relation> processFrom() {
        if (originalSpec.getFrom().isEmpty() || mvSpec.getFrom().isEmpty()) {
            notFit("From empty");
            return Optional.empty();
        }

        Relation origRelation = originalSpec.getFrom().get();
        if (origRelation instanceof AliasedRelation) {
            origRelation = ((AliasedRelation) origRelation).getRelation();
        }

        Relation mvRelation = mvSpec.getFrom().get();
        if (mvRelation instanceof AliasedRelation) {
            mvRelation = ((AliasedRelation) mvRelation).getRelation();
        }

        // simple equal
        if (Objects.equals(origRelation, mvRelation)) {
            // 直接相同的情况下, 使用 mv
            QualifiedObjectName mvName = materializedViewRewriter.getMvName();
            QualifiedName name = QualifiedName.of(mvName.getCatalogName(),
                    mvName.getSchemaName(), mvName.getObjectName());
            Table mvTable = new Table(name);
            return Optional.of(mvTable);
        }
        notFit("表对不上");
        return Optional.empty();
    }

    public boolean isMvFit() {
        return materializedViewRewriter.isMvFit();
    }

    public void notFit(String reason) {
        materializedViewRewriter.notFit(reason);
    }

    public QuerySpecification getOriginalSpec() {
        return originalSpec;
    }

    public QuerySpecification getMvSpec() {
        return mvSpec;
    }

    public MaterializedViewRewriter getMaterializedViewRewriter() {
        return materializedViewRewriter;
    }

    public Map<Expression, QualifiedSingleColumn> getOriginalColumnRefMap() {
        return originalColumnRefMap;
    }

    public Map<Expression, QualifiedSingleColumn> getMvColumnRefMap() {
        return mvColumnRefMap;
    }

    public Map<QualifiedSingleColumn, SelectItem> getMvSelectableColumn() {
        return mvSelectableColumn;
    }

    /**
     * util
     * 获取 node-ResolvedField map
     */
    private static Map<Expression, QualifiedSingleColumn> extractNodeFieldMap(Analysis analysis) {
        Map<NodeRef<Expression>, ResolvedField> columnReferenceFields = analysis.getColumnReferenceFields();
        Map<Expression, QualifiedSingleColumn> map = new HashMap<>(columnReferenceFields.size());

        for (Map.Entry<NodeRef<Expression>, ResolvedField> kv : columnReferenceFields.entrySet()) {
            Expression expr = kv.getKey().getNode();
            ResolvedField resolvedField = kv.getValue();
            QualifiedSingleColumn qualifiedSingleColumn = fromField(resolvedField);
            if (qualifiedSingleColumn != null) {
                map.put(expr, qualifiedSingleColumn);
            }
        }
        return map;
    }

    /**
     * util
     * 从 ResolvedField中提取 QualifiedColumn
     */
    private static QualifiedSingleColumn fromField(ResolvedField resolvedField) {
        Field field = resolvedField.getField();
        Optional<QualifiedObjectName> table = field.getOriginTable();
        Optional<String> column = field.getOriginColumnName();
        if (table.isPresent() && column.isPresent()) {
            return new QualifiedSingleColumn(table.get(), column.get());
        }
        return null; // 表面没有字段
    }

    /**
     * util
     * 抽取 mv select中的 single column, 这些字段可用于后续使用
     */
    private static Map<QualifiedSingleColumn, SelectItem> extractSelectSingleField(QuerySpecification mvSpec, Map<Expression, QualifiedSingleColumn> columnRefMap) {
        Select select = mvSpec.getSelect();
        Map<QualifiedSingleColumn, SelectItem> map = new HashMap<>();

        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) selectItem;
                Expression expression = column.getExpression();

                if (expression instanceof Identifier) {
                    // Identifier 字段可以后续使用 select col1, col2, 这里的 col1, col2就是 Identifier expression
                    // 这些可供 后续使用
                    QualifiedSingleColumn column1 = columnRefMap.get(expression);
                    if (column1 != null) {
                        map.put(column1, selectItem);
                    }
                } else {
                    LOG.warn("TODO: 目前忽略非 SingleColumn [%s] ", expression);
                }
            } else {
                LOG.debug("忽略 非 SingleColumn, 当前selectItem类型=%s", selectItem.getClass().getName());
            }
        }
        return map;
    }

    /**
     * util
     * 获取一个 column的 最后一个部分
     */
    private static Identifier getNameLastPart(SingleColumn column) {
        Optional<Identifier> alias = column.getAlias();
        if (alias.isPresent()) {
            return alias.get();
        }

        Expression expression = column.getExpression();
        if (expression instanceof Identifier) {
            return (Identifier) expression;
        }

        if (expression instanceof DereferenceExpression) {
            Optional<Identifier> field = ((DereferenceExpression) expression).getField();
            if (field.isPresent()) {
                return field.get();
            }
        }

        throw new UnsupportedOperationException("TODO column中获取 最后一部分出错");
    }

    /**
     * 获取 一个 baseTable.column1 在 mv中的对应的字段名
     * 比如
     * create or replace materialized view iceberg.kernel_db01.mv_part_01
     * as
     * SELECT mfgr mfgr2,....
     * from iceberg.kernel_db01.part
     * ...
     * 则 iceberg.tpch_tiny.part.mfgr => iceberg.kernel_db01.mv_part_01.mfgr2
     *
     * @param qualifiedSingleColumn iceberg.tpch_tiny.part.mfgr
     * @return iceberg.kernel_db01.mv_part_01.mfgr2, 被 DereferenceExpression wrap
     */
    public DereferenceExpression correspondColumnInMv(QualifiedSingleColumn qualifiedSingleColumn) {
        SelectItem selectItem = mvSelectableColumn.get(qualifiedSingleColumn);
        if (selectItem == null) {
            return null;
        }
        if (!(selectItem instanceof SingleColumn)) {
            throw new UnsupportedOperationException("TODO 目前仅支持 SingleColumn 查找");
        }
        SingleColumn column = (SingleColumn) selectItem;

        Identifier identifier = column.getAlias().orElseGet(() -> (Identifier) column.getExpression());
        return new DereferenceExpression(mvExpression, identifier);
    }

}
