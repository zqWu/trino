package io.trino.sql.rewrite;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.analyzer.*;
import io.trino.sql.rewritemv.MaterializedViewRewriteHelper;
import io.trino.sql.rewritemv.MaterializedViewOptimization;
import io.trino.sql.tree.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MaterializedViewRewrite implements StatementRewrite.Rewrite {

    @Override
    public Statement rewrite(AnalyzerFactory analyzerFactory, Session session, Statement statement, List<Expression> parameters, Map<NodeRef<Parameter>, Expression> parameterLookup, WarningCollector warningCollector) {

        // only apply to query, ignore else (eg. insert/ update / delete / ddl ...)
        if (!(statement instanceof Query)) {
            return statement;
        }

        MaterializedViewRewriteHelper helper = MaterializedViewRewriteHelper.getInstance();

        // analyze 现在这个
        Analysis originalAnalysis = new Analysis(statement, new HashMap<>(), QueryType.OTHERS);
        StatementAnalyzer analyzer = helper.getStatementAnalyzerFactory()
                .createStatementAnalyzer(originalAnalysis, session, WarningCollector.NOOP, CorrelationSupport.ALLOWED);
        analyzer.analyze(statement, Optional.empty());

        Statement resultStatement = MaterializedViewOptimization.rewrite(session, originalAnalysis);
        return resultStatement;
    }
    // statement.queryBody {QuerySpecification}
}
