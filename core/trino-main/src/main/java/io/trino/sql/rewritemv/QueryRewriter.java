package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.QuerySpecification;
import io.trino.sql.tree.With;

import java.util.Optional;

/**
 * giving mv, rewrite a query if possible
 */
public class QueryRewriter extends AstVisitor<Node, MvDetail> {
    private static final Logger LOG = Logger.get(QueryRewriter.class);
    private final Analysis analysis;
    private final QuerySpecification spec;
    private boolean isMvFit = true;

    public QueryRewriter(Analysis analysis) {
        this.analysis = analysis;
        Query query = (Query) this.analysis.getStatement();
        this.spec = (QuerySpecification) query.getQueryBody();
    }

    @Override
    protected Node visitQuery(Query node, MvDetail mvDetail) {
        // 1. with
        // 2. queryBody
        // 3. orderBy
        // 4. offset
        // 5. limit

        // with
        Optional<With> optionalWith = Optional.empty();
        if (node.getWith().isPresent()) {
            With with = node.getWith().get();
            With newWith = (With) process(with, mvDetail);
            optionalWith = Optional.of(newWith);
        }

        // querySpecification
        QuerySpecification origSpec = (QuerySpecification) node.getQueryBody();
        QuerySpecification newSpec = (QuerySpecification) process(origSpec, mvDetail);

        // orderBy limit offset
        if (mvDetail.getMvQuery().getOrderBy().isPresent()) {
            notFit("not support: mv has [orderBy]");
        }
        if (mvDetail.getMvQuery().getOffset().isPresent()) {
            notFit("not support: mv has [offset]");
        }
        if (mvDetail.getMvQuery().getLimit().isPresent()) {
            notFit("not support: mv has [limit]");
        }

        if (isMvFit()) {
            return new Query(optionalWith, newSpec, node.getOrderBy(), node.getOffset(), node.getLimit());
        } else {
            return analysis.getStatement();
        }
    }

    @Override
    protected Node visitWith(With with, MvDetail mvDetail) {
        if (!isMvFit()) {
            return with;
        }

        Optional<With> o2 = mvDetail.getMvQuery().getWith();
        if (o2.isPresent()) {
            notFit("not support: mv has [With] clause");
        }
        return with;
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, MvDetail mvDetail) {
        QuerySpecificationRewriter rewriter = new QuerySpecificationRewriter(this);
        return rewriter.process(node, mvDetail);
    }

    public boolean isMvFit() {
        return isMvFit;
    }

    public void notFit(String reason) {
        if (reason != null) {
            LOG.debug("notFit [%s]", reason);
        }
        isMvFit = false;
    }

    public Analysis getAnalysis() {
        return analysis;
    }

    public QuerySpecification getSpec() {
        return spec;
    }


}
