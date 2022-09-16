package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.rewritemv.where.EquivalentClass;
import io.trino.sql.rewritemv.where.PredictEqual;
import io.trino.sql.rewritemv.where.PredictOther;
import io.trino.sql.rewritemv.where.PredictRange;
import io.trino.sql.rewritemv.where.WhereAnalysis;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.IsNotNullPredicate;
import io.trino.sql.tree.IsNullPredicate;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.Relation;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;
import io.trino.sql.tree.Table;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

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
            QualifiedColumn qualifiedColumn = fromField(resolvedField);
            if (qualifiedColumn != null) {
                map.put(expr, qualifiedColumn);
            }
        }
        return map;
    }

    /**
     * util
     * 从 ResolvedField中提取 QualifiedColumn
     */
    public static QualifiedColumn fromField(ResolvedField resolvedField) {
        Field field = resolvedField.getField();
        Optional<QualifiedObjectName> table = field.getOriginTable();
        Optional<String> column = field.getOriginColumnName();
        if (table.isPresent() && column.isPresent()) {
            return new QualifiedColumn(table.get(), column.get());
        }
        return null; // 表面没有字段
    }

    /**
     * util
     * 抽取 mv select中的 single column, 这些字段可用于后续使用
     */
    public static Map<QualifiedColumn, SelectItem> extractSelectSingleField(QuerySpecification mvSpec, Map<Expression, QualifiedColumn> columnRefMap) {
        Select select = mvSpec.getSelect();
        Map<QualifiedColumn, SelectItem> map = new HashMap<>();

        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (selectItem instanceof SingleColumn) {
                SingleColumn column = (SingleColumn) selectItem;
                Expression expression = column.getExpression();

                if (expression instanceof Identifier) {
                    // Identifier 字段可以后续使用 select col1, col2, 这里的 col1, col2就是 Identifier expression
                    // 这些可供 后续使用
                    QualifiedColumn column1 = columnRefMap.get(expression);
                    if (column1 != null) {
                        map.put(column1, selectItem);
                    }
                } else {
                    LOG.warn("TODO: ignore non SingleColumn [%s] ", expression);
                }
            } else {
                LOG.debug("TODO: ignore non SingleColumn, selectItem=[%s]", selectItem.getClass().getName());
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

        return list;
    }

    /**
     * @param qCol 列名
     * @param mvDetail
     */
    public static DereferenceExpression findColumnInMv(QualifiedColumn qCol, MvDetail mvDetail) {
        SelectItem selectItem = mvDetail.getSelectableColumn().get(qCol);

        // 如果找不到, 使用 ec 进行查找
        if (selectItem == null) {
            EquivalentClass targetEc = null;
            for (EquivalentClass ec : mvDetail.getWhereAnalysis().getEcList()) {
                if (ec.contain(qCol)) {
                    targetEc = ec;
                    break;
                }
            }

            if (targetEc != null) {
                for (QualifiedColumn c : targetEc.getColumns()) {
                    if (!Objects.equals(qCol, c)) {
                        selectItem = mvDetail.getSelectableColumn().get(c);
                        if (selectItem != null) {
                            break;
                        }
                    }
                }
            }
        }

        if (selectItem == null) {
            return null;
        }

        if (!(selectItem instanceof SingleColumn)) {
            return null;
        }
        SingleColumn column = (SingleColumn) selectItem;

        Identifier identifier = column.getAlias().orElseGet(() -> (Identifier) column.getExpression());
        return new DereferenceExpression(mvDetail.getTableNameExpression(), identifier);
    }

    /**
     * @param ec 多个可能的列名(对等关系)
     * @param mvDetail
     */
    public static DereferenceExpression findColumnInMv(EquivalentClass ec, MvDetail mvDetail) {
        for (QualifiedColumn column : ec.getColumns()) {
            DereferenceExpression refer = findColumnInMv(column, mvDetail);
            if (refer != null) {
                return refer;
            }
        }
        return null;
    }


    /**
     * flatten where to atomic predict
     */
    public static WhereAnalysis analyzeWhere(Expression whereExpr, Map<Expression, QualifiedColumn> map) {
        WhereAnalysis whereAnalysis = new WhereAnalysis();
        WhereVisitor visitor = new WhereVisitor(map);
        visitor.process(whereExpr, whereAnalysis);
        whereAnalysis.build();

        return whereAnalysis;
    }

    private static class WhereVisitor extends AstVisitor<Void, WhereAnalysis> {
        private final Map<Expression, QualifiedColumn> refMap;

        public WhereVisitor(Map<Expression, QualifiedColumn> refMap) {
            this.refMap = refMap;
        }

        @Override
        protected Void visitComparisonExpression(ComparisonExpression node, WhereAnalysis context) {
            // TODO 对于比较, 需要区分是 colA op colB, 还是 colA op value
            // colA=3这样的形式
            Expression left = node.getLeft();
            Expression right = node.getRight();
            ComparisonExpression.Operator op = node.getOperator();
            boolean equalOp = ComparisonExpression.Operator.EQUAL == op;

            // c.s.tableA.colX = 1, colA<1 , 1 >= c.s.tableA.colX,  1=colA
            if ((left instanceof DereferenceExpression || left instanceof Identifier) && right instanceof Literal) {
                if (PredictRange.validOperator(op) && PredictRange.validLiteral((Literal) right)) {
                    QualifiedColumn column1 = refMap.get(left);
                    context.addPredict(new PredictRange(node, column1, (Literal) right, op));
                } else {
                    context.addPredict(new PredictOther(node));
                }
            } else if ((right instanceof DereferenceExpression || right instanceof Identifier) && left instanceof Literal) {
                if (PredictRange.validOperator(op) && PredictRange.validLiteral((Literal) left)) {
                    QualifiedColumn column1 = refMap.get(right);
                    context.addPredict(new PredictRange(node, column1, (Literal) left, op));
                } else {
                    context.addPredict(new PredictOther(node));
                }
            }

            // colA=colB, colB=c.s.tableA.colY,  c.s.tableA.colX=colB, c.s.tableA.colX=c.s.tableA.colY
            else if (equalOp
                    && (left instanceof Identifier || left instanceof DereferenceExpression)
                    && (right instanceof Identifier || right instanceof DereferenceExpression)
            ) {
                QualifiedColumn column1 = refMap.get(left);
                QualifiedColumn column2 = refMap.get(right);
                context.addPredict(new PredictEqual(node, column1, column2));
            }

            // other situation
            else {

                // 3 > 2, '3'>'2'
//                if (left instanceof Literal && right instanceof Literal) {
//                    // context.addPredict(new PredictLiteral(node, (Literal) left, (Literal) right, op));
//                    context.addPredict(new PredictOther(node));
//                }
//
//                // 1+2>2, colA+1=3, 3=colA+1, 30>2+1, 现在不做处理
//                else if (left instanceof ArithmeticBinaryExpression && right instanceof Literal) {
//                    context.addPredict(new PredictOther(node));
//                } else if (right instanceof ArithmeticBinaryExpression && left instanceof Literal) {
//                    context.addPredict(new PredictOther(node));
//                }

                context.addPredict(new PredictOther(node));
            }

            return null;
        }

        @Override
        protected Void visitLogicalExpression(LogicalExpression node, WhereAnalysis context) {
            if (LogicalExpression.Operator.AND == node.getOperator()) {
                for (Expression expr : node.getTerms()) {
                    process(expr, context);
                }
            } else {
                context.notSupport("unsupported: logicExpression OR:" + node);
                // context.notConjunctionMode();
            }
            return null;
        }

        @Override
        protected Void visitIsNotNullPredicate(IsNotNullPredicate node, WhereAnalysis context) {
            context.addPredict(new PredictOther(node));
            return null;
        }

        @Override
        protected Void visitIsNullPredicate(IsNullPredicate node, WhereAnalysis context) {
            context.addPredict(new PredictOther(node));
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, WhereAnalysis context) {
            context.addPredict(new PredictOther(node));
            return null;
        }
    }

}
