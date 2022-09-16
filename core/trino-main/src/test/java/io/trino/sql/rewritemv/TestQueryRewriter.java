package io.trino.sql.rewritemv;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import io.trino.Session;
import io.trino.connector.CatalogServiceProvider;
import io.trino.connector.StaticConnectorFactory;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.*;
import io.trino.plugin.base.security.AllowAllSystemAccessControl;
import io.trino.plugin.base.security.DefaultSystemAccessControl;
import io.trino.security.AccessControl;
import io.trino.security.AccessControlConfig;
import io.trino.security.AccessControlManager;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.Analyzer;
import io.trino.sql.analyzer.AnalyzerFactory;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.ShowQueriesRewrite;
import io.trino.sql.rewrite.StatementRewrite;
import io.trino.testing.LocalQueryRunner;
import io.trino.testing.TestingMetadata;
import io.trino.transaction.TransactionManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.operator.scalar.ApplyFunction.APPLY_FUNCTION;
import static io.trino.spi.session.PropertyMetadata.integerProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static io.trino.sql.analyzer.StatementAnalyzerFactory.createTestingStatementAnalyzerFactory;
import static io.trino.testing.TestingEventListenerManager.emptyEventListenerManager;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

@Test(singleThreaded = true)
public class TestQueryRewriter {
    private static final SqlParser sqlParser = new SqlParser();
    private static final String TPCH_CATALOG = "tpch";
    private static final String SESSION_SCHEMA = "s1";
    private final Closer closer = Closer.create();
    private TransactionManager transactionManager;
    private AccessControl accessControl;
    private PlannerContext plannerContext;
    private TablePropertyManager tablePropertyManager;
    private AnalyzePropertyManager analyzePropertyManager;
    private Metadata metadata;
    private MaterializedViewRewriteHelper helper;
    private static final Session TEST_SESSION = testSessionBuilder()
            .setCatalog(TPCH_CATALOG)
            .setSchema(SESSION_SCHEMA)
//            .setSystemProperty("parse_decimal_literals_as_double", "true")
            .build();


    @Test
    public void test1() {
        System.out.println("test1");


    }

    @BeforeClass
    public void setup() {
        System.out.println("setup");
        LocalQueryRunner queryRunner = LocalQueryRunner.create(TEST_SESSION);
        closer.register(queryRunner);
        transactionManager = queryRunner.getTransactionManager();

        AccessControlManager accessControlManager = new AccessControlManager(
                transactionManager,
                emptyEventListenerManager(),
                new AccessControlConfig(),
                DefaultSystemAccessControl.NAME);
        accessControlManager.setSystemAccessControls(List.of(AllowAllSystemAccessControl.INSTANCE));
        this.accessControl = accessControlManager;

        queryRunner.addFunctions(InternalFunctionBundle.builder().functions(APPLY_FUNCTION).build());
        plannerContext = queryRunner.getPlannerContext();
        metadata = plannerContext.getMetadata();

//        TestingMetadata testingConnectorMetadata = new TestingMetadata();
//        TestingConnector connector = new TestingConnector(testingConnectorMetadata);
//        queryRunner.createCatalog(TPCH_CATALOG, new StaticConnectorFactory("main", connector), ImmutableMap.of());

        tablePropertyManager = queryRunner.getTablePropertyManager();
        analyzePropertyManager = queryRunner.getAnalyzePropertyManager();

        StatementAnalyzerFactory factory = StatementAnalyzerFactory.createTestingStatementAnalyzerFactory(
                plannerContext,
                accessControl,
                tablePropertyManager,
                analyzePropertyManager
        );
        helper = new MaterializedViewRewriteHelper(metadata, sqlParser, factory);
    }

    @AfterClass
    public void tearDown() {
        System.out.println("tearDown");
    }

    private Analyzer createAnalyzer(Session session, AccessControl accessControl) {
        StatementRewrite statementRewrite = new StatementRewrite(ImmutableSet.of(new ShowQueriesRewrite(
                plannerContext.getMetadata(),
                sqlParser,
                accessControl,
                new SessionPropertyManager(),
                new SchemaPropertyManager(CatalogServiceProvider.fail()),
                new ColumnPropertyManager(CatalogServiceProvider.fail()),
                tablePropertyManager,
                new MaterializedViewPropertyManager(catalogName -> ImmutableMap.of()))));
        StatementAnalyzerFactory statementAnalyzerFactory = createTestingStatementAnalyzerFactory(plannerContext, accessControl, tablePropertyManager, analyzePropertyManager);
        AnalyzerFactory analyzerFactory = new AnalyzerFactory(statementAnalyzerFactory, statementRewrite);
        return analyzerFactory.createAnalyzer(
                session,
                emptyList(),
                emptyMap(),
                WarningCollector.NOOP);
    }

    private static class TestingConnector implements Connector {
        private final ConnectorMetadata metadata;

        public TestingConnector(ConnectorMetadata metadata) {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit) {
            return new ConnectorTransactionHandle() {
            };
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
            return metadata;
        }

        @Override
        public List<PropertyMetadata<?>> getAnalyzeProperties() {
            return ImmutableList.of(
                    stringProperty("p1", "test string property", "", false),
                    integerProperty("p2", "test integer property", 0, false));
        }
    }
}
