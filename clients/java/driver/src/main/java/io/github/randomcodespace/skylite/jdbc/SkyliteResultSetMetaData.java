package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Column;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Types;
import java.util.List;
import java.util.Locale;

/**
 * Result columns from a {@code columns} frame: name, declared type and origin. The protocol carries no nullability,
 * precision or scale, so those are reported as unknown.
 */
final class SkyliteResultSetMetaData implements ResultSetMetaData {
    private final List<Column> columns;

    SkyliteResultSetMetaData(List<Column> columns) {
        this.columns = columns;
    }

    /**
     * A declared type's name as SQLite reads it, the Go driver's rule: without the arguments in parentheses, trimmed,
     * in upper case. {@code datetime(6)} is DATETIME. Empty for an expression.
     */
    static String typeName(String decl) {
        if (decl == null) {
            return "";
        }
        int paren = decl.indexOf('(');
        return (paren < 0 ? decl : decl.substring(0, paren)).strip().toUpperCase(Locale.ROOT);
    }

    /**
     * The JDBC type of a declared type: DATE, and TIMESTAMP for DATETIME and TIMESTAMP, which read as temporal
     * values; otherwise SQLite's affinity rules: INT → BIGINT, CHAR, CLOB or TEXT → VARCHAR, BLOB or none → BLOB,
     * REAL, FLOA or DOUB → DOUBLE, anything else → NUMERIC. An expression, with no declared type, is OTHER: its values
     * may be of any storage class.
     */
    static int jdbcType(String decl) {
        if (decl == null) {
            return Types.OTHER;
        }
        String t = typeName(decl);
        switch (t) {
            case "DATE":
                return Types.DATE;
            case "DATETIME", "TIMESTAMP":
                return Types.TIMESTAMP;
            default:
                break;
        }
        if (t.contains("INT")) {
            return Types.BIGINT;
        }
        if (t.contains("CHAR") || t.contains("CLOB") || t.contains("TEXT")) {
            return Types.VARCHAR;
        }
        if (t.contains("BLOB") || t.isEmpty()) {
            return Types.BLOB;
        }
        if (t.contains("REAL") || t.contains("FLOA") || t.contains("DOUB")) {
            return Types.DOUBLE;
        }
        return Types.NUMERIC;
    }

    private Column col(int i) throws SQLException {
        if (i < 1 || i > columns.size()) {
            throw new SQLNonTransientException("skylite: column index " + i + " is outside 1 to " + columns.size(), "07009");
        }
        return columns.get(i - 1);
    }

    @Override
    public int getColumnCount() {
        return columns.size();
    }

    @Override
    public boolean isAutoIncrement(int i) throws SQLException {
        col(i);
        return false;
    }

    @Override
    public boolean isCaseSensitive(int i) throws SQLException {
        return getColumnType(i) == Types.VARCHAR || getColumnType(i) == Types.OTHER;
    }

    @Override
    public boolean isSearchable(int i) throws SQLException {
        col(i);
        return true;
    }

    @Override
    public boolean isCurrency(int i) throws SQLException {
        col(i);
        return false;
    }

    @Override
    public int isNullable(int i) throws SQLException {
        col(i);
        return columnNullableUnknown;
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        int t = getColumnType(i);
        return t == Types.BIGINT || t == Types.DOUBLE || t == Types.NUMERIC;
    }

    @Override
    public int getColumnDisplaySize(int i) throws SQLException {
        col(i);
        return Integer.MAX_VALUE;
    }

    @Override
    public String getColumnLabel(int i) throws SQLException {
        return col(i).name();
    }

    /** The column of the table it comes from, or the label for an expression. */
    @Override
    public String getColumnName(int i) throws SQLException {
        Column c = col(i);
        return c.origin() != null ? c.origin() : c.name();
    }

    @Override
    public String getSchemaName(int i) throws SQLException {
        col(i);
        return "";
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        col(i);
        return 0;
    }

    @Override
    public int getScale(int i) throws SQLException {
        col(i);
        return 0;
    }

    @Override
    public String getTableName(int i) throws SQLException {
        Column c = col(i);
        return c.table() == null ? "" : c.table();
    }

    @Override
    public String getCatalogName(int i) throws SQLException {
        col(i);
        return "";
    }

    @Override
    public int getColumnType(int i) throws SQLException {
        return jdbcType(col(i).declType());
    }

    @Override
    public String getColumnTypeName(int i) throws SQLException {
        return typeName(col(i).declType());
    }

    @Override
    public boolean isReadOnly(int i) throws SQLException {
        col(i);
        return true;
    }

    @Override
    public boolean isWritable(int i) throws SQLException {
        col(i);
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int i) throws SQLException {
        col(i);
        return false;
    }

    /** The class getObject returns for the column's usual values; a value's own storage class can differ. */
    @Override
    public String getColumnClassName(int i) throws SQLException {
        return switch (getColumnType(i)) {
            case Types.BIGINT -> Long.class.getName();
            case Types.DOUBLE -> Double.class.getName();
            case Types.VARCHAR -> String.class.getName();
            case Types.BLOB -> byte[].class.getName();
            case Types.DATE -> java.sql.Date.class.getName();
            case Types.TIMESTAMP -> java.sql.Timestamp.class.getName();
            default -> Object.class.getName();
        };
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLNonTransientException("skylite: not a wrapper for " + iface.getName(), "HY000");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface.isInstance(this);
    }
}
