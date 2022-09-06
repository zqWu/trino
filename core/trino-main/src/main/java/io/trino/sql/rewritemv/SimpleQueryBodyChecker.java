package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.tree.*;

import java.util.Objects;
import java.util.Optional;

public class SimpleQueryBodyChecker {
    private static final Logger LOG = Logger.get(SimpleQueryBodyChecker.class);

    public static boolean isEqual(QuerySpecification qs1, QuerySpecification qs2) {
        // sql 的执行顺序
        // from join on
        // where
        // groupBy + aggregate function
        // having
        // windows
        // select
        // distinct
        // union / intersect / except
        // orderBy
        // offset
        // limit
        if (!checkFrom(qs1, qs2)) {
            return false;
        }

        if (!checkWhere(qs1, qs2)) {
            return false;
        }

        if (!checkGroupBy(qs1, qs2)) {
            return false;
        }

        if (!checkSelect(qs1, qs2)) {
            return false;
        }

        return true;
    }

    private static boolean checkSelect(QuerySpecification qs1, QuerySpecification qs2) {
        Select s1 = qs1.getSelect();
        Select s2 = qs2.getSelect();

        if (!Objects.equals(s1, s2)) {
            return false;
        }
        return true;
    }

    private static boolean checkGroupBy(QuerySpecification qs1, QuerySpecification qs2) {
        Optional<GroupBy> o1 = qs1.getGroupBy();
        Optional<GroupBy> o2 = qs2.getGroupBy();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        if (o1.isEmpty() || o2.isEmpty()) {
            return false;
        }
        GroupBy v1 = o1.get();
        GroupBy v2 = o2.get();
        if (!Objects.equals(v1, v2)) {
            return false;
        }
        if (!checkHaving(qs1, qs2)) {
            return false;
        }

        return true;
    }

    // TODO having进行判断, 类似 where判断
    private static boolean checkHaving(QuerySpecification qs1, QuerySpecification qs2) {
        Optional<Expression> o1 = qs1.getHaving();
        Optional<Expression> o2 = qs2.getHaving();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        if (o1.isEmpty() || o2.isEmpty()) {
            return false;
        }
        Expression v1 = o1.get();
        Expression v2 = o2.get();
        if (!Objects.equals(v1, v2)) {
            return false;
        }
        return true;
    }

    // TODO 根据论文 where这里需要进行非常细致的判断, 这里先略过了
    private static boolean checkWhere(QuerySpecification qs1, QuerySpecification qs2) {
        Optional<Expression> o1 = qs1.getWhere();
        Optional<Expression> o2 = qs2.getWhere();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        if (o1.isEmpty() || o2.isEmpty()) {
            return false;
        }

        Expression v1 = o1.get();
        Expression v2 = o2.get();

        // simple expression
        if (Objects.equals(v1, v2)) {
            return true;
        }

        return false;
    }

    private static boolean checkFrom(QuerySpecification qs1, QuerySpecification qs2) {
        Optional<Relation> o1 = qs1.getFrom();
        Optional<Relation> o2 = qs2.getFrom();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        if (o1.isEmpty() || o2.isEmpty()) {
            return false;
        }

        Relation v1 = o1.get();
        Relation v2 = o2.get();
        // simple equal
        if (Objects.equals(v1, v2)) {
            return true;
        }

        LOG.info("TODO currently not support: Relation compare");
        return false;
    }
}
