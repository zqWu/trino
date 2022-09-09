package io.trino.sql.rewritemv;

import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.sql.analyzer.Analysis;
import io.trino.sql.rewritemv.rewriter.MaterializedViewRewriter;
import io.trino.sql.tree.Statement;

public class MaterializedViewOptimization {
    private static final Logger LOG = Logger.get(MaterializedViewOptimization.class);

    public static Statement rewrite(Session session, Analysis original) {

        for (MvEntry entry : MaterializedViewLoader.getMv(session).values()) {
            MaterializedViewRewriter rewriter = new MaterializedViewRewriter(original, entry.analysis, entry.name);
            rewriter.process();
            if (rewriter.isMvFit()) {
                LOG.debug("=====> !!! found mv fit sql !!!");
                return rewriter.getOutStatement();
            }
        }

        LOG.debug("no mv fit sql");
        return original.getStatement();
    }

}
