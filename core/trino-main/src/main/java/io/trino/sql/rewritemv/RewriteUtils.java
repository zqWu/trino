package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.tree.*;

import java.util.*;

public class RewriteUtils {
    public static final Logger LOG = Logger.get(RewriteUtils.class);

    /**
     * util
     * 获取 node-ResolvedField map
     */
    public static Map<Expression, QualifiedSingleColumn> extractNodeFieldMap(Analysis analysis) {
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
    public static QualifiedSingleColumn fromField(ResolvedField resolvedField) {
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
    public static Map<QualifiedSingleColumn, SelectItem> extractSelectSingleField(QuerySpecification mvSpec, Map<Expression, QualifiedSingleColumn> columnRefMap) {
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

        throw new UnsupportedOperationException("TODO column中获取 最后一部分出错");
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

        return list;
    }

    public static DereferenceExpression correspondColumnInMv(QualifiedSingleColumn qualifiedSingleColumn, MvDetail mvDetail) {
        SelectItem selectItem = mvDetail.getSelectableColumn().get(qualifiedSingleColumn);
        if (selectItem == null) {
            return null;
        }
        if (!(selectItem instanceof SingleColumn)) {
            throw new UnsupportedOperationException("TODO 目前仅支持 SingleColumn 查找");
        }
        SingleColumn column = (SingleColumn) selectItem;

        Identifier identifier = column.getAlias().orElseGet(() -> (Identifier) column.getExpression());
        return new DereferenceExpression(mvDetail.getTableNameExpression(), identifier);
    }

}
