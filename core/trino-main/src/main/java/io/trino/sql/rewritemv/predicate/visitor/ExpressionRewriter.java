package io.trino.sql.rewritemv.predicate.visitor;

import io.trino.sql.tree.AstVisitor;
import io.trino.sql.tree.Expression;

public abstract class ExpressionRewriter
        extends AstVisitor<Expression, Void>
{
}
