package io.trino.sql.rewrite;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.analyzer.*;
import io.trino.sql.rewritemv.MaterializedViewLoader;
import io.trino.sql.rewritemv.MaterializedViewRewriteHelper;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QueryRewriter;
import io.trino.sql.tree.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MaterializedViewRewrite implements StatementRewrite.Rewrite {
    private static final Logger LOG = Logger.get(MaterializedViewRewrite.class);

    @Override
    public Statement rewrite(AnalyzerFactory analyzerFactory, Session session, Statement statement, List<Expression> parameters, Map<NodeRef<Parameter>, Expression> parameterLookup, WarningCollector warningCollector) {

        // only apply to query, ignore else (eg. insert/ update / delete / ddl ...)
        if (!(statement instanceof Query)) {
            return statement;
        }

        MaterializedViewRewriteHelper helper = MaterializedViewRewriteHelper.getInstance();

        // analyze 现在这个
        Analysis originalAnalysis = new Analysis(statement, new HashMap<>(), QueryType.OTHERS); // TODO 异常
        StatementAnalyzer analyzer = helper.getStatementAnalyzerFactory()
                .createStatementAnalyzer(originalAnalysis, session, WarningCollector.NOOP, CorrelationSupport.ALLOWED);
        analyzer.analyze(statement, Optional.empty());

        Statement resultStatement = rewrite(session, originalAnalysis);
        return resultStatement;
    }
    // statement.queryBody {QuerySpecification}


    private static Statement rewrite(Session session, Analysis original) {
        Statement result = original.getStatement();
        for (MvDetail entry : MaterializedViewLoader.getMv(session).values()) {
            QueryRewriter rewriter = new QueryRewriter(original);
            result = (Statement) rewriter.process(original.getStatement(), entry);
            if (rewriter.isMvFit()) {
                LOG.info("=====> !!! found mv fit sql [%s] !!!", entry.getMvName());
                return result;
            }
        }

        LOG.info("no mv fit sql");
        return result;
    }
}
