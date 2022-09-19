package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.jsonwebtoken.lang.Collections;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.rewritemv.where.EquivalentClass;
import io.trino.sql.rewritemv.where.WhereRewriter;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static io.trino.sql.rewritemv.RewriteUtils.extractColumnReferenceMap;
import static io.trino.sql.rewritemv.RewriteUtils.getNameLastPart;

/**
 * rewrite a specification by using a given mv if possible
 */
public class QuerySpecificationRewriter extends AstVisitor<Node, MvDetail> {
    private static final Logger LOG = Logger.get(QuerySpecificationRewriter.class);
    private final QueryRewriter queryRewriter;
    private final Map<Expression, QualifiedColumn> columnRefMap;

    // ecList & mvSelectableColumnExpand 是 select处理完毕后得到的产物
    private List<EquivalentClass> ecList;
    private Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend;

    public QuerySpecificationRewriter(QueryRewriter queryRewriter) {
        this.queryRewriter = queryRewriter;
        columnRefMap = extractColumnReferenceMap(queryRewriter.getAnalysis());
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
        Select select = processSelect(node, mvDetail, groupByRewriter.getGroupColumns());

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

    private Select processSelect(QuerySpecification origSpec, MvDetail mvDetail, Set<QualifiedColumn> groupColumns) {
        QuerySpecification mvSpec = mvDetail.getQuerySpecification();
        Select origSelect = origSpec.getSelect();
        Select mvSelect = mvSpec.getSelect();

        if (!Objects.equals(origSelect.isDistinct(), mvSelect.isDistinct())) {
            notFit("select cannot match, one has distinct, another not");
            return origSelect;
        }

        List<SelectItem> rewrite = new ArrayList<>();
        for (SelectItem origSelectItem : origSelect.getSelectItems()) {
            if (origSelectItem instanceof AllColumns) {
                notFit("not support: original select has AllColumns(like *)");
                continue;
            }

            // orig: select t1.a, ...
            // mv:   select t1.a as t1a, ....
            // 修改:  select mv.t1a as a .... from mv
            SingleColumn origColumn = (SingleColumn) origSelectItem;
            Expression expression = origColumn.getExpression();
            QualifiedColumn qCol = columnRefMap.get(expression);
            if (qCol == null) {
                // 不涉及字段, 直接加入. 如 select count(1), 这里的 count(1)
                rewrite.add(origSelectItem);
                continue;
            }

            SingleColumn mvCol = (SingleColumn) mvSelectableColumnExtend.get(qCol);
            if (mvCol == null) {
                notFit("column not present in mv:" + qCol);
                return origSelect;
            }

            Identifier mvColumnLast = getNameLastPart(mvCol);
            Identifier origColumnLast = getNameLastPart(origColumn);
            // 获取mv中该字段的名称
            // 这个用mv字段名, ru iceberg.tpch_tiny.orders
            Expression eee = new DereferenceExpression(mvDetail.getTableNameExpression(), mvColumnLast);
            SingleColumn a1 = new SingleColumn(eee, origColumnLast);
            rewrite.add(a1);

        }

        Select s = new Select(origSelect.isDistinct(), rewrite);
        return s;
    }

    private Optional<Expression> processWhere(MvDetail mvDetail) {
        WhereRewriter whereRewriter = new WhereRewriter(this, mvDetail);
        Expression expression = whereRewriter.process();
        if (isMvFit()) {
            ecList = whereRewriter.getWhereAnalysis().getEcList();
            mvSelectableColumnExtend = whereRewriter.getMvSelectableColumnExtend();
        }

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
        notFit("not match: table not match");
        return node;
    }

    public EquivalentClass getEquivalentClassByColumn(QualifiedColumn column) {
        if (Collections.isEmpty(ecList)) {
            return null;
        }

        for (EquivalentClass ec : ecList) {
            if (ec.contain(column)) {
                return ec;
            }
        }
        return null;
    }

    public boolean isMvFit() {
        return queryRewriter.isMvFit();
    }

    public void notFit(String reason) {
        queryRewriter.notFit(reason);
    }

    // ======== set get

    public List<EquivalentClass> getEcList() {
        return ecList;
    }

    public QueryRewriter getQueryRewriter() {
        return queryRewriter;
    }

    public Map<Expression, QualifiedColumn> getColumnRefMap() {
        return columnRefMap;
    }

    public Map<QualifiedColumn, SelectItem> getMvSelectableColumnExtend() {
        return mvSelectableColumnExtend;
    }
}
