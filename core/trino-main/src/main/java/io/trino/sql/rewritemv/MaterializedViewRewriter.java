package io.trino.sql.rewritemv;

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.*;
import io.trino.sql.ParsingUtil;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.CorrelationSupport;
import io.trino.sql.analyzer.QueryType;
import io.trino.sql.analyzer.StatementAnalyzer;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Statement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MaterializedViewRewriter {
    private static final Map<QualifiedObjectName, MvEntry> mvCache = new HashMap<>();

    public static Analysis rewrite(Session session, Analysis original) {
        getMv(session);

        return original;
    }

    /**
     * TODO this function should execute once on startup. how to do this
     */
    public static void getMv(Session session) {
        Metadata metadata = MaterializedViewRewriteHelper.getInstance().getMetadata();
        List<CatalogInfo> catalogInfos = metadata.listCatalogs(session);

        for (CatalogInfo catalogInfo : catalogInfos) {
            QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogInfo.getCatalogName());
            Map<QualifiedObjectName, ViewInfo> materializedViews = metadata.getMaterializedViews(session, prefix);

            for (QualifiedObjectName name : materializedViews.keySet()) {
                if (!mvCache.containsKey(name)) {
                    addToCache(session, name, materializedViews.get(name));
                }
            }
        }
    }

    private static synchronized void addToCache(Session session, QualifiedObjectName name, ViewInfo viewInfo) {
        MaterializedViewRewriteHelper helper = MaterializedViewRewriteHelper.getInstance();

        String mvSql = viewInfo.getOriginalSql();
        // refer QueryPreparer::prepareQuery(Session, String query)
        Statement statement = helper.getSqlParser().createStatement(mvSql, ParsingUtil.createParsingOptions(session));

        // assume that materialized view could be restore from its original sql
        Map<NodeRef<Parameter>, Expression> emptyLookup = new HashMap<>();
        Analysis analysis = new Analysis(statement, emptyLookup, QueryType.OTHERS);
        StatementAnalyzer analyzer = helper.getStatementAnalyzerFactory()
                .createStatementAnalyzer(analysis, session, WarningCollector.NOOP, CorrelationSupport.ALLOWED);
        analyzer.analyze(statement, Optional.empty());

        MvEntry entry = new MvEntry(name, viewInfo, analysis);
        mvCache.put(name, entry);
    }

    private static class MvEntry {
        final QualifiedObjectName name;
        final ViewInfo viewInfo;
        final Analysis analysis;

        public MvEntry(QualifiedObjectName name, ViewInfo viewInfo, Analysis analysis) {
            this.name = name;
            this.viewInfo = viewInfo;
            this.analysis = analysis;
        }
    }
}
