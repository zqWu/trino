package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.*;
import io.trino.sql.ParsingUtil;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.CorrelationSupport;
import io.trino.sql.analyzer.QueryType;
import io.trino.sql.analyzer.StatementAnalyzer;
import io.trino.sql.tree.Statement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MaterializedViewRewriter {
    private static final Logger LOG = Logger.get(MaterializedViewRewriter.class);
    private static boolean sync = false;
    private static final Map<QualifiedObjectName, MvEntry> mvCache = new HashMap<>();

    public static Analysis rewrite(Session session, Analysis original) {
        loadMaterializedViewOnlyOnce(session);

        for (MvEntry entry : mvCache.values()) {
            Analysis mvAnalysis = entry.analysis;
            MaterializedViewMatcher matcher = new MaterializedViewMatcher(mvAnalysis, original);
            if (matcher.match()) {
                LOG.debug("materialized view matches \n%s", entry.viewInfo.getOriginalSql());
                // after match, rewrite original statement TODO
                return mvAnalysis;
            }
        }

        return original;
    }

    /**
     * TODO this function should execute once on startup. how to do this
     * get all materialized view definition and their Analysis
     */
    private static void loadMaterializedViewOnlyOnce(Session session) {
        if (sync) {
            return;
        }

        sync = true;
        Metadata metadata = MaterializedViewRewriteHelper.getInstance().getMetadata();
        List<CatalogInfo> catalogInfoList = metadata.listCatalogs(session);

        for (CatalogInfo catalogInfo : catalogInfoList) {
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
        Analysis analysis = new Analysis(statement, new HashMap<>(), QueryType.OTHERS);
        StatementAnalyzer analyzer = helper.getStatementAnalyzerFactory()
                .createStatementAnalyzer(analysis, session, WarningCollector.NOOP, CorrelationSupport.ALLOWED);
        analyzer.analyze(statement, Optional.empty());

        MvEntry entry = new MvEntry(name, statement, viewInfo, analysis);
        mvCache.put(name, entry);
    }

    private static class MvEntry {
        final QualifiedObjectName name;
        final Statement statement;

        final ViewInfo viewInfo;
        final Analysis analysis;

        public MvEntry(QualifiedObjectName name, Statement statement, ViewInfo viewInfo, Analysis analysis) {
            this.name = name;
            this.statement = statement;
            this.viewInfo = viewInfo;
            this.analysis = analysis;
        }
    }
}
