package io.trino.sql.rewritemv.predicate.visitor;

import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.AllColumns;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SelectRewriteVisitor
        extends AstVisitor<Select, Void>
{
    private final HavingRewriteVisitor expressionVisitor;

    private boolean isFit = true;
    private String reason = null;

    public SelectRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
            Map<Expression, QualifiedColumn> columnRefMap,
            MvDetail mvDetail,
            boolean isMvGrouped)
    {
        expressionVisitor = new HavingRewriteVisitor(mvSelectableColumnExtend, columnRefMap, mvDetail, isMvGrouped);
    }

    @Override
    protected Select visitSelect(Select node, Void context)
    {
        List<SelectItem> selectItems = node.getSelectItems();
        List<SelectItem> rewrite = new ArrayList<>();

        for (SelectItem origSelectItem : selectItems) {
            if (origSelectItem instanceof AllColumns) {
                reason = "select not support: " + node;
                isFit = false;
                return node;
            }

            SingleColumn origColumn = (SingleColumn) origSelectItem;
            Expression expressBefore = origColumn.getExpression();
            Expression expressAfter = expressionVisitor.process(expressBefore, context);
            if (expressAfter == null) {
                reason = "select not support: " + node;
                isFit = false;
                return node;
            }

            SingleColumn after1 = new SingleColumn(expressAfter, origColumn.getAlias());
            rewrite.add(after1);
        }

        return new Select(node.isDistinct(), rewrite);
    }

    // ======== get
    public boolean isFit()
    {
        return isFit;
    }

    public String getReason()
    {
        return reason;
    }
}
