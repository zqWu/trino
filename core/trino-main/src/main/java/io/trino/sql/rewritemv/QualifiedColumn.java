package io.trino.sql.rewritemv;

import io.trino.metadata.QualifiedObjectName;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * 一个全限定 单个字段名
 */
public class QualifiedColumn
        implements Comparable<QualifiedColumn>
{
    private final QualifiedObjectName table;
    private final String columnName;

    @Override
    public int compareTo(@NotNull QualifiedColumn o)
    {
        // 比较 catalogName
        int result = table.getCatalogName().compareTo(o.getTable().getCatalogName());
        if (result != 0) {
            return result;
        }

        // 比较 schemaName
        result = table.getSchemaName().compareTo(o.getTable().getSchemaName());
        if (result != 0) {
            return result;
        }

        // 比较 ObjectName
        result = table.getObjectName().compareTo(o.getTable().getObjectName());
        if (result != 0) {
            return result;
        }

        // 比较 columnName
        return columnName.compareTo(o.columnName);
    }

    public QualifiedColumn(QualifiedObjectName table, String columnName)
    {
        requireNonNull(table, "table is null");
        requireNonNull(table.getCatalogName(), "table.catalog is null");
        requireNonNull(table.getSchemaName(), "table.schema is null");
        requireNonNull(table.getObjectName(), "table.object is null");
        requireNonNull(columnName, "table is null");

        this.table = table;
        this.columnName = columnName;
    }

    public QualifiedObjectName getTable()
    {
        return table;
    }

    public String getColumnName()
    {
        return columnName;
    }

    @Override
    public String toString()
    {
        return table + "." + columnName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QualifiedColumn)) {
            return false;
        }
        QualifiedColumn that = (QualifiedColumn) o;
        return Objects.equals(table, that.table) && Objects.equals(columnName, that.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, columnName);
    }
}
