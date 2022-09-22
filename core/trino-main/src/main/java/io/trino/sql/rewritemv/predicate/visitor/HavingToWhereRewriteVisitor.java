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
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * 改写 having Expression的 Visitor类
 * - column替换
 * - 函数支持
 */
public class HavingToWhereRewriteVisitor extends WhereColumnRewriteVisitor {
    private final List<String> SUPPORT_FUNCTION = Arrays.asList("avg", "count", "max", "min", "sum");
    private static final Logger LOG = Logger.get(HavingToWhereRewriteVisitor.class);

    public HavingToWhereRewriteVisitor(Map<QualifiedColumn, SelectItem> mvSelectableColumnExtend,
                                       Map<Expression, QualifiedColumn> columnRefMap,
                                       MvDetail mvDetail) {
        super(mvSelectableColumnExtend, columnRefMap, mvDetail);
    }

    @Override
    protected Expression visitFunctionCall(FunctionCall node, Void context) {
        QualifiedName funName = node.getName();
        String name = funName.getSuffix();

        if (!SUPPORT_FUNCTION.contains(name)) {
            return null;
        }
        switch (name) {
            case "max":
            case "min":
            case "sum":
            case "avg": // 这里 avg是直接查找, 还不支持 sum/count 来计算
                return processOneArgFunction(node, node.getName());
            case "count":
                return processFunctionCount(node, node.getName());
            default:
                // TODO
                return null;
        }
    }

    /**
     * count 有这些种: count(1), count(col), count(*)
     * 可能有0/1个参数
     */
    private Expression processFunctionCount(FunctionCall node, QualifiedName funName) {
        List<Expression> arguments = node.getArguments();
        QualifiedColumn columnArg = null;
        if (arguments.size() == 0) {
            // count(*)
            columnArg = null;
        } else {
            Expression arg1 = arguments.get(0);
            // count(常数) or count(colA)
            if (arg1 instanceof Identifier || arg1 instanceof DereferenceExpression) {
                columnArg = origColumnRefMap.get(arg1);
                if (columnArg == null) {
                    LOG.debug(String.format("mv中未找到需要的字段 %s", arg1));
                    return null;
                }
            } else if (arg1 instanceof Literal) {
                columnArg = null;
            }
        }

        return findAndRewriteSelectItemIfPossible(funName, columnArg);
    }

    /**
     * max/min/sum/avg 有且只有一个 参数(arg)
     * having max(colA) > 10  ======>  where max_colA > 10
     *
     * @param funName max
     * @param node like FunctionCall("max", "price")
     */
    private Expression processOneArgFunction(FunctionCall node, QualifiedName funName) {
        List<Expression> arguments = node.getArguments();
        Expression arg1 = arguments.get(0);

        if (arg1 instanceof Identifier || arg1 instanceof DereferenceExpression) {
            // 在 mv中 查找 max(arg1) 这个selectable
            QualifiedColumn columnArg = origColumnRefMap.get(arg1);
            if (columnArg == null) {
                LOG.debug(String.format("mv中未找到需要的字段 %s", arg1));
                return null;
            }
            return findAndRewriteSelectItemIfPossible(funName, columnArg);
            // return new FunctionCall(funName, List.of(d));
        } else {
            // 比如 having max(colA+colB) > 10 这种结构, 暂不支持
            LOG.warn("max function has complex expression, not support");
            return null;
        }
    }

    @Nullable
    private Expression findAndRewriteSelectItemIfPossible(QualifiedName funName, QualifiedColumn columnArg) {
        SelectItem selectItem = findSelectItemInMv(funName, columnArg);
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
    public SelectItem findSelectItemInMv(QualifiedName funName, QualifiedColumn columnArg) {
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
            } else {
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

}
