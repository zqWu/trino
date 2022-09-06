package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.*;

import java.util.Optional;

/**
 * compare two analysis, if two are same
 */
public class MaterializedViewMatcher {
    private static final Logger LOG = Logger.get(MaterializedViewMatcher.class);
    private final Analysis mv;
    private final Analysis mayRewrite;

    public MaterializedViewMatcher(Analysis mv, Analysis mayRewrite) {
        this.mv = mv;
        this.mayRewrite = mayRewrite;
    }

    public boolean match() {
        Query q1 = (Query) mv.getStatement();
        Statement s2 = mayRewrite.getStatement();
        if (!(s2 instanceof Query)) {
            return false;
        }

        Query q2 = (Query) mayRewrite.getStatement();

        return compareQuery(q1, q2);
    }

    private boolean compareQuery(Query q1, Query q2) {
        // here are node to compare:
        // 1. with
        // 2. queryBody
        // 3. orderBy
        // 4. offset
        // 5. limit

        // traversal the tree
        if (!checkWith(q1, q2)) {
            return false;
        }

        if (!checkQueryBody(q1, q2)) {
            return false;
        }

        if (!checkOrderBy(q1, q2)) {
            return false;
        }

        if (!checkOffset(q1, q2)) {
            return false;
        }

        if (!checkLimit(q1, q2)) {
            return false;
        }

        return true;
    }

    private boolean checkQueryBody(Query q1, Query q2) {
        QueryBody b1 = q1.getQueryBody();
        QueryBody b2 = q2.getQueryBody();
        if (!(b1 instanceof QuerySpecification) || !(b2 instanceof QuerySpecification)) {
            return false;
        }
        return SimpleQueryBodyChecker.isEqual((QuerySpecification) b1, (QuerySpecification) b2);
    }

    private boolean checkOrderBy(Query q1, Query q2) {
        Optional<OrderBy> o1 = q1.getOrderBy();
        Optional<OrderBy> o2 = q2.getOrderBy();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        LOG.debug("TODO OrderBy check: currently not support");
        return false;
    }

    private boolean checkOffset(Query q1, Query q2) {
        Optional<Offset> o1 = q1.getOffset();
        Optional<Offset> o2 = q2.getOffset();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        LOG.debug("TODO Offset check: currently not support");
        return false;
    }

    private boolean checkLimit(Query q1, Query q2) {
        Optional<Node> o1 = q1.getLimit();
        Optional<Node> o2 = q2.getLimit();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        LOG.debug("TODO Limit check: currently not support");
        return false;
    }

    private boolean checkWith(Query q1, Query q2) {
        Optional<With> o1 = q1.getWith();
        Optional<With> o2 = q2.getWith();
        if (o1.isEmpty() && o2.isEmpty()) {
            return true;
        }
        LOG.debug("TODO With check: currently not support");
        return false;
    }

}
