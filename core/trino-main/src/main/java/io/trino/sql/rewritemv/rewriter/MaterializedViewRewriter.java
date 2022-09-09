package io.trino.sql.rewritemv.rewriter;

import io.airlift.log.Logger;
import io.trino.metadata.QualifiedObjectName;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.*;

import java.util.Optional;

/**
 * compare two analysis, if two are same
 */
public class MaterializedViewRewriter {
    private static final Logger LOG = Logger.get(MaterializedViewRewriter.class);
    private final Analysis originalAnalysis;
    private final Analysis mvAnalysis;
    private final QualifiedObjectName mvName;
    private boolean isMvFit;
    private Statement outStatement;

    public MaterializedViewRewriter(Analysis originalAnalysis, Analysis mvAnalysis, QualifiedObjectName mvName) {
        this.originalAnalysis = originalAnalysis;
        this.mvAnalysis = mvAnalysis;
        this.mvName = mvName;
    }

    public void process() {
        FastFilter fastFilter = new FastFilter(originalAnalysis, mvAnalysis);
        isMvFit = true; // fastFilter.process();
        if (isMvFit()) {
            LOG.debug("fastFilter 未通过");
        }
        outStatement = doProcess();
    }

    private Statement doProcess() {
        Statement s1 = originalAnalysis.getStatement();
        Statement s2 = mvAnalysis.getStatement();
        if (!(s1 instanceof Query) || !(s2 instanceof Query)) {
            notFit("statement should be query");
            return originalAnalysis.getStatement();
        } else {
            Statement s = originalAnalysis.getStatement();
            try {
                s = processQuery((Query) s1, (Query) s2);
            } catch (Exception ex) {
                LOG.warn("rewrite阶段异常", ex);
            }
            return s;
        }
    }

    protected Query processQuery(Query orig, Query mv) {
        if (!isMvFit()) {
            return (Query) originalAnalysis.getStatement();
        }

        // here are node to compare:
        // 1. with
        // 2. queryBody
        // 3. orderBy
        // 4. offset
        // 5. limit

        // traversal the tree
        Optional<With> optionalWith = processWith(orig, mv);
        QuerySpecification spec = processQuerySpecification(orig, mv);
        Optional<OrderBy> orderBy = processOrderBy(orig, mv);
        Optional<Offset> offset = processOffset(orig, mv);
        Optional<Node> limit = processLimit(orig, mv);

        if (isMvFit()) {
            return new Query(optionalWith, spec, orderBy, offset, limit);
        } else {
            return (Query) originalAnalysis.getStatement();
        }
    }

    private QuerySpecification processQuerySpecification(Query orig, Query mv) {
        if (!isMvFit()) {
            return null;
        }

        QueryBody origSpec = orig.getQueryBody();
        QueryBody mvSpec = mv.getQueryBody();
        if (!(origSpec instanceof QuerySpecification) || !(mvSpec instanceof QuerySpecification)) {
            return null;
        }

        QuerySpecRewriter querySpecRewriter = new QuerySpecRewriter(this);

        return querySpecRewriter.process();
    }

    private Optional<OrderBy> processOrderBy(Query orig, Query mv) {
        if (!isMvFit()) {
            return Optional.empty();
        }
        Optional<OrderBy> o1 = orig.getOrderBy();
        Optional<OrderBy> o2 = mv.getOrderBy();
        if (o1.isEmpty() && o2.isEmpty()) {
            return o1;
        } else {
            LOG.debug("TODO: 目前还不支持非空 [OrderBy] 子句");
            return Optional.empty();
        }
    }

    private Optional<Offset> processOffset(Query orig, Query mv) {
        if (!isMvFit()) {
            return Optional.empty();
        }
        Optional<Offset> o1 = orig.getOffset();
        Optional<Offset> o2 = mv.getOffset();
        if (o1.isEmpty() && o2.isEmpty()) {
            return o1;
        } else {
            LOG.debug("TODO: 目前还不支持非空 [Offset] 子句");
            return Optional.empty();
        }
    }

    private Optional<Node> processLimit(Query orig, Query mv) {
        if (!isMvFit()) {
            return Optional.empty();
        }
        Optional<Node> o1 = orig.getLimit();
        Optional<Node> o2 = mv.getLimit();
        if (o1.isEmpty() && o2.isEmpty()) {
            return o1;
        } else {
            LOG.debug("TODO: 目前还不支持非空 [Limit] 子句");
            return Optional.empty();
        }
    }

    private Optional<With> processWith(Query orig, Query mv) {
        if (!isMvFit()) {
            return Optional.empty();
        }

        Optional<With> o1 = orig.getWith();
        Optional<With> o2 = mv.getWith();
        if (o1.isEmpty() && o2.isEmpty()) {
            return o1;
        } else {
            LOG.debug("TODO: 目前还不支持非空 [With] 子句");
            return Optional.empty();
        }
    }

    public boolean isMvFit() {
        return isMvFit;
    }

    public void notFit(String reason) {
        if (reason != null) {
            LOG.debug("notFit, reason=%s", reason);
        }
        isMvFit = false;
    }

    public Analysis getOriginalAnalysis() {
        return originalAnalysis;
    }

    public Analysis getMvAnalysis() {
        return mvAnalysis;
    }

    public Statement getOutStatement() {
        return outStatement;
    }

    public QualifiedObjectName getMvName() {
        return mvName;
    }
}
