package io.trino.sql.rewrite;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.CorrelationSupport;
import io.trino.sql.analyzer.QueryType;
import io.trino.sql.analyzer.StatementAnalyzer;
import io.trino.sql.rewritemv.MaterializedViewLoader;
import io.trino.sql.rewritemv.MaterializedViewRewriteHelper;
import io.trino.sql.rewritemv.MvDetail;
import io.trino.sql.rewritemv.QueryRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Query;
import io.trino.sql.tree.Statement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MaterializedViewRewrite
        implements StatementRewrite.Rewrite
{
    private static final Logger LOG = Logger.get(MaterializedViewRewrite.class);

    @Override
    public Statement rewrite(AnalyzerFactory analyzerFactory,
            Session session,
            Statement statement,
            List<Expression> parameters,
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            WarningCollector warningCollector)
    {

        int status = SystemSessionProperties.queryRewriteWithMaterializedViewStatus(session);
        if (status == 0) {
            return statement;
        }
        if (status == 2) {
            MaterializedViewLoader.invalidate();
        }

        // only apply to query, ignore else (eg. insert/ update / delete / ddl ...)
        if (!(statement instanceof Query)) {
            return statement;
        }

        MaterializedViewRewriteHelper helper = MaterializedViewRewriteHelper.getInstance();

        // analyze 现在这个
        try {
            Analysis originalAnalysis = new Analysis(statement, new HashMap<>(), QueryType.OTHERS);
            StatementAnalyzer analyzer = helper.getStatementAnalyzerFactory()
                    .createStatementAnalyzer(originalAnalysis, session, WarningCollector.NOOP, CorrelationSupport.ALLOWED);
            analyzer.analyze(statement, Optional.empty());

            Statement resultStatement = rewrite(session, originalAnalysis);
            return resultStatement;
        }
        catch (Exception ex) {
            // if any exception happens, use original statement
            // original query has some error
            return statement;
        }
    }

    private static Statement rewrite(Session session, Analysis original)
    {
        if (original.getStatement() == null) {
            return null;
        }

        Statement result = original.getStatement();
        for (MvDetail entry : MaterializedViewLoader.getMv(session).values()) {
            QueryRewriter rewriter = new QueryRewriter(original, session);
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
