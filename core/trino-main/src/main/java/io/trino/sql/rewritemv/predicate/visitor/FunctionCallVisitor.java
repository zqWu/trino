package io.trino.sql.rewritemv.predicate.visitor;

import io.airlift.log.Logger;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QualifiedColumn;
import io.trino.sql.tree.DereferenceExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Literal;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.Select;
import io.trino.sql.tree.SelectItem;
import io.trino.sql.tree.SingleColumn;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class FunctionCallVisitor
        extends WhereColumnRewriteVisitor
{
    protected static final Logger LOG = Logger.get(FunctionCallVisitor.class);
    protected static final List<String> SUPPORTED_FUNCTION = Arrays.asList("avg", "count", "max", "min", "sum");
    protected static final QualifiedName FUNCTION_SUM = QualifiedName.of("sum");
    protected static final QualifiedName FUNCTION_COUNT = QualifiedName.of("count");

    public FunctionCallVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
            Map<Expression, QualifiedColumn> origColumnRefMap, MvDetail mvDetail)
    {
        super(mvSelectableColumnExtend, origColumnRefMap, mvDetail);
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, Void context)
    {
        // mv自身进行了 groupBy, 对 having 需要做处理
        QualifiedName funName = node.getName();
        String name = funName.getSuffix();

        if (!SUPPORTED_FUNCTION.contains(name)) {
            LOG.debug("不支持的函数:" + name);
            return null;
        }

        Expression after = doVisitFunctionCall(node, context);
        if (after != null) {
            LOG.debug(String.format("having rewrite: %s ===> %s", node, after));
        }
        else {
            LOG.debug(String.format("having rewrite: fail %s", node));
        }
        return after;
    }

    protected abstract Expression doVisitFunctionCall(FunctionCall node, Void context);

    /**
     * 查找并改写 mv中使用 function(arg)的 selectItem.
     * 如 mv: select max(age) as max_age, 则获得次 selectItem的信息
     *
     * @param funName 函数名, 如 max/min/avg/sum/count
     * @param columnArg 函数列作为参数
     * @return null if not possible
     */
    protected final Expression findAndRewriteSelectItemIfPossible(QualifiedName funName, QualifiedColumn columnArg)
    {
        SelectItem selectItem = findSelectItemInMvUseFunction(funName, columnArg);
        if (selectItem == null) {
            LOG.debug(String.format("mv中未找到 %s(%s)", funName.getSuffix(), columnArg));
            return null;
        }
        SingleColumn funWithColumn = (SingleColumn) selectItem;
        if (funWithColumn.getAlias().isEmpty()) {
            LOG.debug(String.format("mv %s(%s) 未使用别名, 无法select", funName.getSuffix(), columnArg));
            return null;
        }

        return new DereferenceExpression(mvDetail.getTableNameExpression(), funWithColumn.getAlias().get());
    }

    /**
     * 查找 mv 中 select clause 中的 funName(columnArg)
     *
     * @param funName 函数名, 如 max/min/sum/avg/count
     * @param columnArg 函数参数, columnArg
     * @return null if not found
     */
    protected final SelectItem findSelectItemInMvUseFunction(QualifiedName funName, QualifiedColumn columnArg)
    {
        Select select = mvDetail.getMvQuerySpec().getSelect();
        List<SelectItem> selectItems = select.getSelectItems();

        for (SelectItem selectItem : selectItems) {
            if (!(selectItem instanceof SingleColumn)) {
                LOG.debug("TODO: ignore non SingleColumn, selectItem=[%s]", selectItem.getClass().getName());
                continue;
            }

            SingleColumn column = (SingleColumn) selectItem;
            Expression expression = column.getExpression();

            // if match, must be same function
            if (!(expression instanceof FunctionCall)) {
                continue;
            }
            FunctionCall fun = (FunctionCall) expression;
            if (!Objects.equals(fun.getName(), funName)) {
                continue;
            }

            // if match, arguments must be logic same
            List<Expression> arguments = fun.getArguments();
            if (columnArg == null) {
                if (arguments.size() == 0) {
                    return selectItem;
                }
            }
            else {
                if (arguments.size() != 1) {
                    continue;
                }

                Expression arg1 = arguments.get(0);
                if (!(arg1 instanceof Identifier || arg1 instanceof DereferenceExpression)) {
                    continue;
                }
                QualifiedColumn funArg = mvDetail.getMvColumnRefMap().get(arg1);
                if (Objects.equals(funArg, columnArg)) {
                    return selectItem;
                }
            }
        }

        return null;
    }

    /**
     * count 函数的处理
     * count 有这些种: count(1), count(col), count(*)
     *
     * @param node FunctionCall
     * @param funName count
     * @param enableCountAll 如果查找不到 count(colA), 是否允许 count(*) 替代, 注意 count(*) = count(1) = count(-99), 但是 count(colA) 与 不统计 colA=null的行, count(1) != count(colA)
     * @return 替换后的函数, null = fail to rewrite
     */
    protected Expression processFunctionCount(FunctionCall node, QualifiedName funName, boolean enableCountAll)
    {
        List<Expression> arguments = node.getArguments();
        QualifiedColumn columnArg = null;
        if (arguments.size() == 0) { // case: count(*)
            columnArg = null;
        }
        else { // case: count(常数) or count(colA)
            Expression arg1 = arguments.get(0);
            if (arg1 instanceof Identifier || arg1 instanceof DereferenceExpression) {
                columnArg = origColumnRefMap.get(arg1);
                if (columnArg == null) {
                    LOG.debug(String.format("mv中未找到需要的字段 %s", arg1));
                    return null;
                }
            }
            else if (arg1 instanceof Literal) {
                columnArg = null;
            }
        }

        Expression expr = findAndRewriteSelectItemIfPossible(funName, columnArg);
        if (expr == null && columnArg != null && enableCountAll) {
            // 刚刚尝试的是 count(colA), 找不到再试一下 col(*)
            expr = findAndRewriteSelectItemIfPossible(funName, null);
        }

        if (expr == null) {
            LOG.debug(String.format("having 无法处理 %s(%s)", funName.getSuffix(), columnArg));
        }
        return expr;
    }
}
