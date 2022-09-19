package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.Field;
import io.trino.sql.analyzer.ResolvedField;
import io.trino.sql.rewritemv.where.EquivalentClass;
import io.trino.sql.rewritemv.where.PredicateEqual;
import io.trino.sql.rewritemv.where.PredicateOther;
import io.trino.sql.rewritemv.where.PredicateRange;
import io.trino.sql.rewritemv.where.WhereAnalysis;
import io.trino.sql.tree.AliasedRelation;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.BetweenPredicate;
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
                                                                            WhereAnalysis whereAnalysis) {
        Select select = spec.getSelect();
        Map<QualifiedColumn, SelectItem> map = new HashMap<>();

        List<SelectItem> selectItems = select.getSelectItems();
        for (SelectItem selectItem : selectItems) {
            if (!(selectItem instanceof SingleColumn)) {
                LOG.debug("TODO: ignore non SingleColumn, selectItem=[%s]", selectItem.getClass().getName());
                continue;
            }

            SingleColumn column = (SingleColumn) selectItem;
            Expression expression = column.getExpression();
            if (expression instanceof Identifier) {
                // select col1, col2, 这里的 col1, col2就是 Identifier expression
                QualifiedColumn column1 = columnRefMap.get(expression);
                if (column1 != null) {
                    map.put(column1, selectItem);
                }
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

    public static DereferenceExpression findColumnInMv(QualifiedColumn col, Map<QualifiedColumn, SelectItem> colMap, DereferenceExpression table) {
        SelectItem selectItem = colMap.get(col);
        if (selectItem == null) {
            return null;
        }

        if (!(selectItem instanceof SingleColumn)) {
            return null;
        }
        SingleColumn column = (SingleColumn) selectItem;

        Identifier identifier = column.getAlias().orElseGet(() -> (Identifier) column.getExpression());
        return new DereferenceExpression(table, identifier);
    }


    /**
     * flatten where to atomic predicate
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
                if (PredicateRange.validOperator(op) && PredicateRange.validLiteral((Literal) right)) {
                    QualifiedColumn column1 = refMap.get(left);
                    context.addPredicate(new PredicateRange(node, column1, (Literal) right, op));
                } else {
                    context.addPredicate(new PredicateOther(node));
                }
            } else if ((right instanceof DereferenceExpression || right instanceof Identifier) && left instanceof Literal) {
                if (PredicateRange.validOperator(op) && PredicateRange.validLiteral((Literal) left)) {
                    QualifiedColumn column1 = refMap.get(right);
                    context.addPredicate(new PredicateRange(node, column1, (Literal) left, op));
                } else {
                    context.addPredicate(new PredicateOther(node));
                }
            }

            // colA=colB, colB=c.s.tableA.colY,  c.s.tableA.colX=colB, c.s.tableA.colX=c.s.tableA.colY
            else if (equalOp
                    && (left instanceof Identifier || left instanceof DereferenceExpression)
                    && (right instanceof Identifier || right instanceof DereferenceExpression)
            ) {
                QualifiedColumn column1 = refMap.get(left);
                QualifiedColumn column2 = refMap.get(right);
                context.addPredicate(new PredicateEqual(node, column1, column2));
            }

            // other situation
            else {

                // 3 > 2, '3'>'2'
//                if (left instanceof Literal && right instanceof Literal) {
//                    // context.addPredicate(new PredicateLiteral(node, (Literal) left, (Literal) right, op));
//                    context.addPredicate(new PredictOther(node));
//                }
//
//                // 1+2>2, colA+1=3, 3=colA+1, 30>2+1, 现在不做处理
//                else if (left instanceof ArithmeticBinaryExpression && right instanceof Literal) {
//                    context.addPredicate(new PredictOther(node));
//                } else if (right instanceof ArithmeticBinaryExpression && left instanceof Literal) {
//                    context.addPredicate(new PredictOther(node));
//                }

                context.addPredicate(new PredicateOther(node));
            }

            return null;
        }

        @Override
        protected Void visitBetweenPredicate(BetweenPredicate node, WhereAnalysis context) {
            Expression value = node.getValue();
            Expression min = node.getMin();
            Expression max = node.getMax();
            if ((value instanceof DereferenceExpression || value instanceof Identifier)
                    && min instanceof Literal
                    && max instanceof Literal) {

                QualifiedColumn column = refMap.get(value);
                context.addPredicate(PredicateRange.fromRange(node, column, (Literal) min, (Literal) max));
            } else {
                context.addPredicate(new PredicateOther(node));
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
            context.addPredicate(new PredicateOther(node));
            return null;
        }

        @Override
        protected Void visitIsNullPredicate(IsNullPredicate node, WhereAnalysis context) {
            context.addPredicate(new PredicateOther(node));
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, WhereAnalysis context) {
            context.addPredicate(new PredicateOther(node));
            return null;
        }
    }

}
