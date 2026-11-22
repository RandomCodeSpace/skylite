package io.github.randomcodespace.skylite.jdbc;

import java.nio.charset.CharacterCodingException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.List;

/** The bodies of the stateless requests of ADR 0005: query and describe. Errors never hold a value. */
final class Request {
    private Request() {}

    /** {@code {"sql":...,"params":[...],"min_txn":n,"timeout_ms":n}}; zero and empty fields are left out. */
    static byte[] query(String sql, List<Object> params, long minTxn, long timeoutMs) throws SQLException {
        StringBuilder sb = start(sql);
        if (!params.isEmpty()) {
            sb.append(",\"params\":[");
            for (int i = 0; i < params.size(); i++) {
                if (i > 0) {
                    sb.append(',');
                }
                Values.append(sb, params.get(i));
            }
            sb.append(']');
        }
        if (minTxn > 0) {
            sb.append(",\"min_txn\":").append(minTxn);
        }
        if (timeoutMs > 0) {
            sb.append(",\"timeout_ms\":").append(Math.min(timeoutMs, Integer.MAX_VALUE));
        }
        return finish(sb);
    }

    /** {@code {"sql":...,"min_txn":n}}. */
    static byte[] describe(String sql, long minTxn) throws SQLException {
        StringBuilder sb = start(sql);
        if (minTxn > 0) {
            sb.append(",\"min_txn\":").append(minTxn);
        }
        return finish(sb);
    }

    private static StringBuilder start(String sql) throws SQLException {
        if (sql == null || sql.isEmpty() || sql.indexOf('\0') >= 0) {
            throw new SQLNonTransientException("skylite: the SQL must be non-empty and hold no U+0000", "42000");
        }
        StringBuilder sb = new StringBuilder(sql.length() + 32).append("{\"sql\":");
        Values.appendString(sb, sql);
        return sb;
    }

    private static byte[] finish(StringBuilder sb) throws SQLException {
        try {
            return Values.utf8(sb.append('}').toString());
        } catch (CharacterCodingException e) {
            throw new SQLNonTransientException(
                    "skylite: the SQL or a parameter holds a lone surrogate, which UTF-8 cannot carry", "22021");
        }
    }
}
