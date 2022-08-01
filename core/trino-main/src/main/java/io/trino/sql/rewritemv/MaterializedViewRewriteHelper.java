package io.trino.sql.rewritemv;

import com.google.inject.Inject;
import io.trino.metadata.Metadata;
import io.trino.sql.parser.SqlParser;

public class MaterializedViewRewriteHelper {
    private static MaterializedViewRewriteHelper instance;
    private final Metadata metadata;
    private final SqlParser sqlParser;

    @Inject
    public MaterializedViewRewriteHelper(Metadata metadata, SqlParser sqlParser) {
        this.metadata = metadata;
        this.sqlParser = sqlParser;
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
}
