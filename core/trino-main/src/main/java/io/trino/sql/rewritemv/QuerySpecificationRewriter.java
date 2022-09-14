package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.tree.*;

import java.util.*;

import static io.trino.sql.rewritemv.RewriteUtils.extractNodeFieldMap;
import static io.trino.sql.rewritemv.RewriteUtils.getNameLastPart;

/**
 * rewrite a specification by using a given mv if possible
 */
class QuerySpecificationRewriter extends AstVisitor<Node, MvDetail> {
    private static final Logger LOG = Logger.get(QuerySpecificationRewriter.class);
    private final QueryRewriter queryRewriter;
    private final Map<Expression, QualifiedSingleColumn> originalColumnRefMap;

    public QuerySpecificationRewriter(QueryRewriter queryRewriter) {
        this.queryRewriter = queryRewriter;
        originalColumnRefMap = extractNodeFieldMap(queryRewriter.getAnalysis());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, MvDetail mvDetail) {
        if (!isMvFit()) {
            return node;
        }
        if (node.getFrom().isEmpty()) {
            return node;
        }

        // from
        Relation relation = (Relation) process(node.getFrom().get(), mvDetail);

        // where
        Optional<Expression> where = processWhere(mvDetail);

        // group having
        GroupByRewriter groupByRewriter = new GroupByRewriter(this, mvDetail);
        groupByRewriter.process();

        // select
        Select select = processSelect(node, mvDetail);

        QuerySpecification spec = node;
        if (isMvFit()) {
            // TODO
            spec = new QuerySpecification(
                    select,
                    Optional.of(relation),
                    where, // TODO 需要合并 groupByWriter中的 where
                    groupByRewriter.getResultGroupBy(),
                    groupByRewriter.getHaving(),
                    node.getWindows(),
                    node.getOrderBy(),
                    node.getOffset(),
                    node.getLimit());
        }
        return spec;
    }

    private Select processSelect(QuerySpecification originalSpec, MvDetail mvDetail) {
        QuerySpecification mvSpec = mvDetail.getQuerySpecification();
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
                    SingleColumn mvColumn = (SingleColumn) mvDetail.getSelectableColumn().get(qColumn);
                    if (mvColumn == null) {
                        notFit("原sql中的 select 字段无法在 mv中获取");
                        return origSelect;
                    }

                    Identifier mvColumnLast = getNameLastPart(mvColumn);
                    Identifier origColumnLast = getNameLastPart(origColumn);
                    // 获取mv中该字段的名称
                    Expression eee = new DereferenceExpression(mvDetail.getTableNameExpression(), mvColumnLast); // 这个用mv字段名, ru iceberg.tpch_tiny.orders
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

    private Optional<Expression> processWhere(MvDetail mvDetail) {
        WhereRewriter whereRewriter = new WhereRewriter(this, mvDetail);
        Expression expression = whereRewriter.process();
        if (expression == null) {
            return Optional.empty();
        } else {
            return Optional.of(expression);
        }
    }

    // currently only support simple table
    @Override
    protected Node visitRelation(Relation node, MvDetail mvDetail) {
        if (node instanceof AliasedRelation) {
            node = ((AliasedRelation) node).getRelation();
        }

        Relation mvRelation = mvDetail.getQuerySpecification().getFrom().get();
        if (mvRelation instanceof AliasedRelation) {
            mvRelation = ((AliasedRelation) mvRelation).getRelation();
        }

        // simple equal
        if (Objects.equals(node, mvRelation)) {
            // 直接相同的情况下, 使用 mv
            QualifiedObjectName mvName = mvDetail.getName();
            QualifiedName name = QualifiedName.of(mvName.getCatalogName(), mvName.getSchemaName(), mvName.getObjectName());
            Table mvTable = new Table(name);
            return mvTable;
        }
        notFit("表对不上");
        return node;
    }

    public boolean isMvFit() {
        return queryRewriter.isMvFit();
    }

    public void notFit(String reason) {
        queryRewriter.notFit(reason);
    }

    public QueryRewriter getQueryRewriter() {
        return queryRewriter;
    }

    public Map<Expression, QualifiedSingleColumn> getOriginalColumnRefMap() {
        return originalColumnRefMap;
    }
}
