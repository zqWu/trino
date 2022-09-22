package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.rewritemv.predicate.EquivalentClass;
import io.trino.sql.rewritemv.predicate.PredicateAnalysis;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class RewriteUtils {
    public static final Logger LOG = Logger.get(RewriteUtils.class);

    /**
     * util
     * 获取 node-ResolvedField map
     */
    public static Map<Expression, QualifiedColumn> extractColumnReferenceMap(Analysis analysis) {
        Map<NodeRef<Expression>, ResolvedField> columnReferenceFields = analysis.getColumnReferenceFields();
        Map<Expression, QualifiedColumn> map = new HashMap<>(columnReferenceFields.size());

        for (Map.Entry<NodeRef<Expression>, ResolvedField> kv : columnReferenceFields.entrySet()) {
            Expression expr = kv.getKey().getNode();
            ResolvedField resolvedField = kv.getValue();
            QualifiedColumn qualifiedColumn = extractFromResolvedField(resolvedField);
            if (qualifiedColumn != null) {
                map.put(expr, qualifiedColumn);
            }
        }
        return Collections.unmodifiableMap(map);
    }

    /**
     * util
     * 从 ResolvedField中提取 QualifiedColumn, 没有字段则返回null
     */
    private static QualifiedColumn extractFromResolvedField(ResolvedField resolvedField) {
        Field field = resolvedField.getField();
        Optional<QualifiedObjectName> table = field.getOriginTable();
        Optional<String> column = field.getOriginColumnName();
        if (table.isPresent() && column.isPresent()) {
            return new QualifiedColumn(table.get(), column.get());
        }
        return null;
    }

    /**
     * util
     * 抽取 mv select中的 single column, 这些字段可用于后续使用
     */
    public static Map<QualifiedColumn, SelectItem> extractSelectSingleField(QuerySpecification spec,
                                                                            Map<Expression, QualifiedColumn> columnRefMap,
                                                                            PredicateAnalysis whereAnalysis) {
        Select select = spec.getSelect();
        Map<QualifiedColumn, SelectItem> map = new HashMap<>();

        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (!(selectItem instanceof SingleColumn)) {
                // LOG.debug("ignore non SingleColumn, selectItem=[%s]", selectItem.getClass().getName());
                continue;
            }

            SingleColumn column = (SingleColumn) selectItem;
            Expression expression = column.getExpression();
            if (expression instanceof Identifier || expression instanceof DereferenceExpression) {
                // TODO Dereference,
                // select col1, col2, 这里的 col1, col2就是 Identifier expression
                QualifiedColumn column1 = columnRefMap.get(expression);
                if (column1 != null) {
                    map.put(column1, selectItem);
                }
            } else if (expression instanceof FunctionCall) {
                LOG.warn("TODO: ignore non SingleColumn [%s] ", expression);
            } else {
                LOG.warn("TODO: ignore non SingleColumn [%s] ", expression);
            }
        }

        return extendSelectableColumnByEc(map, whereAnalysis.getEcList());
    }

    // 把 ec加到 selectable中去, 比如 select colA ... where colA=colB, 则 colA和 colB都是 selectable
    public static Map<QualifiedColumn, SelectItem> extendSelectableColumnByEc(Map<QualifiedColumn, SelectItem> in, List<EquivalentClass> ecList) {
        if (ecList == null || ecList.size() == 0) {
            return in;
        }

        Map<QualifiedColumn, SelectItem> tmp = new HashMap<>(in);
        Set<QualifiedColumn> columnIn = in.keySet(); // 原来可以select的 column

        for (EquivalentClass ec : ecList) {
            Set<QualifiedColumn> columns = ec.getColumns();
            Optional<QualifiedColumn> any = columns.stream()
                    .filter(c -> columnIn.contains(c)).findAny();
            if (any.isPresent()) {
                QualifiedColumn delegation = any.get();
                List<QualifiedColumn> ecNotInSelectable = columns.stream()
                        .filter(c -> !columnIn.contains(c))
                        .collect(Collectors.toList());
                if (ecNotInSelectable.size() > 0) {
                    SelectItem sameItemOfEc = in.get(delegation);
                    ecNotInSelectable.forEach(c -> tmp.put(c, sameItemOfEc));
                }
            }
        }
        return Collections.unmodifiableMap(tmp);
    }

    /**
     * util
     * 获取一个 column的 最后一个部分
     */
    public static Identifier getNameLastPart(SingleColumn column) {
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

        throw new UnsupportedOperationException("error occurs when get last part of a column");
    }

    public static List<Table> extractBaseTable(Relation relation) {
        List<Table> list = new ArrayList<>();

        if (relation instanceof AliasedRelation) {
            relation = ((AliasedRelation) relation).getRelation();
        }

        if (relation instanceof Table) {
            list.add((Table) relation);
        } else {
            // TODO
        }

        return Collections.unmodifiableList(list);
    }

    public static DereferenceExpression findColumnInMv(QualifiedColumn col,
                                                       Map<QualifiedColumn, SelectItem> colMap, DereferenceExpression table) {
        SelectItem selectItem = colMap.get(col);
        if (selectItem == null) {
            return null;
        }

        if (!(selectItem instanceof SingleColumn)) {
            return null;
        }
        SingleColumn column = (SingleColumn) selectItem;

        // column 有如下几种形式 (select 后面的 形式)
        // select colA ================================ identifier =======================> 得到 [c.s.t].colA
        // select colA as colX ======================== identifier + alias ===============> 得到 [c.s.t].colX
        // select catalog.schema.table.colA =========== DereferenceExpression ============> 得到 [c.s.t].colA
        // select catalog.schema.table.colA as colX === DereferenceExpression  + alias ===> 得到 [c.s.t].colX


        Identifier identifier = null;
        if (column.getAlias().isPresent()) {
            identifier = column.getAlias().get();
        } else {
            Expression columnExpression = column.getExpression();
            if (columnExpression instanceof DereferenceExpression) {
                identifier = ((DereferenceExpression) columnExpression).getField().get();
            } else {
                identifier = (Identifier) columnExpression;
            }
        }

        return new DereferenceExpression(table, identifier);
    }

}
