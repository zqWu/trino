package io.trino.sql.rewrite;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.sql.analyzer.*;
import io.trino.sql.rewritemv.MaterializedViewRewriteHelper;
import io.trino.sql.rewritemv.MaterializedViewRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MaterializedViewRewrite implements StatementRewrite.Rewrite {

    @Override
    public Statement rewrite(AnalyzerFactory analyzerFactory, Session session, Statement statement, List<Expression> parameters, Map<NodeRef<Parameter>, Expression> parameterLookup, WarningCollector warningCollector) {
        MaterializedViewRewriteHelper helper = MaterializedViewRewriteHelper.getInstance();

        // analyze 现在这个
        Analysis originalAnalysis = new Analysis(statement, new HashMap<>(), QueryType.OTHERS);
        StatementAnalyzer analyzer = helper.getStatementAnalyzerFactory()
                .createStatementAnalyzer(originalAnalysis, session, WarningCollector.NOOP, CorrelationSupport.ALLOWED);
        analyzer.analyze(statement, Optional.empty());

        Analysis a2 = MaterializedViewRewriter.rewrite(session, originalAnalysis);
        Statement resultStatement = a2.getStatement();
        return resultStatement;
    }
}
