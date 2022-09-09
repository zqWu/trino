package io.trino.sql.rewritemv;

import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.ViewInfo;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.tree.Statement;

class MvEntry {
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
