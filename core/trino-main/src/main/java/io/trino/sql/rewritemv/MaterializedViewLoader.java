package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.CatalogInfo;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.QualifiedTablePrefix;
import io.trino.metadata.ViewInfo;
import io.trino.sql.ParsingUtil;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.analyzer.CorrelationSupport;
import io.trino.sql.analyzer.QueryType;
import io.trino.sql.analyzer.StatementAnalyzer;
import io.trino.sql.tree.Statement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * TODO 未完成
 */
public class MaterializedViewLoader {
    private static final Logger LOG = Logger.get(MaterializedViewLoader.class);
    private static boolean sync = false;
    private static Map<QualifiedObjectName, MvDetail> mvCache = new HashMap<>();

    private static List<QualifiedObjectName> skipList = Arrays.asList(
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part01_1"),
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part02_1"),
//
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part03_1"),
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part03_2"),
//
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part04_1"),
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part04_2"),
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part04_3"),
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part04_4"),
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part04_5"),
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part04_6"),
//
//            new QualifiedObjectName("iceberg", "kernel_db01", "mv_part05_1"),

            new QualifiedObjectName("iceberg", "kernel_db01", "na")
    );

    public static void invalidate() {
        if (sync) {
            LOG.debug("invalidate mv cache");
            mvCache.clear();
            sync = false;
        }
    }

    public static Map<QualifiedObjectName, MvDetail> getMv(Session session) {
        loadMaterializedViewOnlyOnce(session);
        return mvCache;
    }

    /**
     * TODO
     * 1 how to execute once on startup. how to do this
     * 2 how to invalidate cache
     * get all materialized view definition and their Analysis
     */
    private static void loadMaterializedViewOnlyOnce(Session session) {
        if (sync) {
            return;
        }

        long begin = System.currentTimeMillis();
        sync = true;
        Metadata metadata = MaterializedViewRewriteHelper.getInstance().getMetadata();
        List<CatalogInfo> catalogInfoList = metadata.listCatalogs(session);

        for (CatalogInfo catalogInfo : catalogInfoList) {
            QualifiedTablePrefix prefix = new QualifiedTablePrefix(catalogInfo.getCatalogName());
            Map<QualifiedObjectName, ViewInfo> materializedViews = metadata.getMaterializedViews(session, prefix);

            for (QualifiedObjectName name : materializedViews.keySet()) {
                if (skipList.contains(name)) {
                    continue;
                }

                if (!mvCache.containsKey(name)) {
                    addToCache(session, name, materializedViews.get(name));
                }
            }
        }
        long cost = System.currentTimeMillis() - begin;
        LOG.debug(String.format("load mv time cost=[%s]ms", cost));
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

        MvDetail entry = new MvDetail(name, statement, viewInfo, analysis);
        mvCache.put(name, entry);
    }

    public static void initForTest(Map<QualifiedObjectName, MvDetail> cache) {
        mvCache = cache;
        sync = true;
    }

}
