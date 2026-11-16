package io.github.randomcodespace.skylite.jdbc;

import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Types;

/**
 * Parameters from a {@code describe}: their number. SQLite parameters have no declared type, so every parameter is
 * an IN parameter of unknown type and nullability.
 */
final class SkyliteParameterMetaData implements ParameterMetaData {
    private final int count;

    SkyliteParameterMetaData(int count) {
        this.count = count;
    }

    private void check(int i) throws SQLException {
        if (i < 1 || i > count) {
            throw new SQLNonTransientException("skylite: parameter index " + i + " is outside 1 to " + count, "07009");
        }
    }

    @Override
    public int getParameterCount() {
        return count;
    }

    @Override
    public int isNullable(int i) throws SQLException {
        check(i);
        return parameterNullableUnknown;
    }

    @Override
    public boolean isSigned(int i) throws SQLException {
        check(i);
        return true;
    }

    @Override
    public int getPrecision(int i) throws SQLException {
        check(i);
        return 0;
    }

    @Override
    public int getScale(int i) throws SQLException {
        check(i);
        return 0;
    }

    @Override
    public int getParameterType(int i) throws SQLException {
        check(i);
        return Types.OTHER;
    }

    @Override
    public String getParameterTypeName(int i) throws SQLException {
        check(i);
        return "";
    }

    @Override
    public String getParameterClassName(int i) throws SQLException {
        check(i);
        return Object.class.getName();
    }

    @Override
    public int getParameterMode(int i) throws SQLException {
        check(i);
        return parameterModeIn;
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
