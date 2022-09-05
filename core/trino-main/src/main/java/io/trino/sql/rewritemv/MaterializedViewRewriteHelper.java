package io.trino.sql.rewritemv;

import com.google.inject.Inject;
import io.trino.metadata.Metadata;
import io.trino.sql.analyzer.StatementAnalyzerFactory;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.rewrite.StatementRewrite;

import static java.util.Objects.requireNonNull;

public class MaterializedViewRewriteHelper {
    private static MaterializedViewRewriteHelper instance;
    private final Metadata metadata;
    private final SqlParser sqlParser;
    private final StatementAnalyzerFactory statementAnalyzerFactory;

    @Inject
    public MaterializedViewRewriteHelper(Metadata metadata, SqlParser sqlParser, StatementAnalyzerFactory statementAnalyzerFactory) {
        this.metadata = metadata;
        this.sqlParser = sqlParser;
        this.statementAnalyzerFactory = statementAnalyzerFactory;
        MaterializedViewRewriteHelper.instance = this;
    }

    public static MaterializedViewRewriteHelper getInstance() {
        return instance;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public SqlParser getSqlParser() {
        return sqlParser;
    }

    public StatementAnalyzerFactory getStatementAnalyzerFactory() {
        return statementAnalyzerFactory;
    }
}
