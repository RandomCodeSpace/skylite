package io.github.randomcodespace.skylite.jdbc;

import java.nio.charset.CharacterCodingException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.List;

/** The request bodies of ADR 0005: query, describe, a channel's and resolve. Errors never hold a value. */
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

    /** {@code {"mode":"write"}}, or {@code {"mode":"read_only","min_txn":n}}: a channel's open. */
    static byte[] open(boolean readOnly, long minTxn) {
        String s = !readOnly ? "{\"mode\":\"write\"}"
                : minTxn > 0 ? "{\"mode\":\"read_only\",\"min_txn\":" + minTxn + "}" : "{\"mode\":\"read_only\"}";
        return s.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }

    /**
     * A statement on a channel: {@code {"channel":t,"seq":n,"op":"execute","sql":...,"params":[...],"min_txn":n,
     * "timeout_ms":n}}, or with begin {@code {"channel":t,"seq":n,"op":"begin","min_txn":n,"timeout_ms":n,
     * "execute":{"sql":...,"params":[...]}}}; zero and empty fields are left out.
     */
    static byte[] execute(String channel, long seq, boolean begin, String sql, List<Object> params, long minTxn,
            long timeoutMs) throws SQLException {
        StringBuilder sb = channel(channel, seq).append(",\"op\":").append(begin ? "\"begin\"" : "\"execute\"");
        sb.append(begin ? ",\"execute\":{" : ",");
        statement(sb, sql, params);
        if (begin) {
            sb.append('}');
        }
        if (minTxn > 0) {
            sb.append(",\"min_txn\":").append(minTxn);
        }
        if (timeoutMs > 0) {
            sb.append(",\"timeout_ms\":").append(Math.min(timeoutMs, Integer.MAX_VALUE));
        }
        return finish(sb);
    }

    /** {@code {"channel":t,"seq":n,"op":op}}: commit or rollback. */
    static byte[] step(String channel, long seq, String op) throws SQLException {
        return finish(channel(channel, seq).append(",\"op\":\"").append(op).append('"'));
    }

    /** {@code {"channel":t,"seq":n,"op":"begin","min_txn":n}}: a begin without a first statement. */
    static byte[] begin(String channel, long seq, long minTxn) throws SQLException {
        StringBuilder sb = channel(channel, seq).append(",\"op\":\"begin\"");
        if (minTxn > 0) {
            sb.append(",\"min_txn\":").append(minTxn);
        }
        return finish(sb);
    }

    /** A batch entry of param_sets: the parameter set as a JSON array. */
    static String paramSet(List<Object> params) throws SQLException {
        StringBuilder sb = new StringBuilder().append('[');
        for (int i = 0; i < params.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            Values.append(sb, params.get(i));
        }
        return sb.append(']').toString();
    }

    /** A batch entry of statements: {@code {"sql":...}}. */
    static String statement(String sql) throws SQLException {
        StringBuilder sb = new StringBuilder(sql == null ? 16 : sql.length() + 16).append('{');
        statement(sb, sql, List.of());
        return sb.append('}').toString();
    }

    /**
     * {@code {"channel":t,"seq":n,"op":"batch","sql":...,"param_sets":[...],"min_txn":n,"timeout_ms":n}}, or with
     * {@code "statements":[...]} when sql is null; entries are those of {@link #paramSet} or {@link #statement}.
     */
    static byte[] batch(String channel, long seq, String sql, List<String> entries, long minTxn, long timeoutMs)
            throws SQLException {
        StringBuilder sb = channel(channel, seq).append(",\"op\":\"batch\",");
        if (sql != null) {
            checkSql(sql);
            sb.append("\"sql\":");
            Values.appendString(sb, sql);
            sb.append(",\"param_sets\":[");
        } else {
            sb.append("\"statements\":[");
        }
        sb.append(String.join(",", entries)).append(']');
        if (minTxn > 0) {
            sb.append(",\"min_txn\":").append(minTxn);
        }
        if (timeoutMs > 0) {
            sb.append(",\"timeout_ms\":").append(Math.min(timeoutMs, Integer.MAX_VALUE));
        }
        return finish(sb);
    }

    /** {@code {"channel":t,"seq":n}}: a cancel's body, and a resolve's. */
    static byte[] cancel(String channel, long seq) throws SQLException {
        return finish(channel(channel, seq));
    }

    /** {@code {"channel":t}}. */
    static byte[] close(String channel) throws SQLException {
        StringBuilder sb = new StringBuilder(channel.length() + 16).append("{\"channel\":");
        Values.appendString(sb, channel);
        return finish(sb);
    }

    private static StringBuilder channel(String channel, long seq) {
        StringBuilder sb = new StringBuilder(channel.length() + 64).append("{\"channel\":");
        Values.appendString(sb, channel);
        return sb.append(",\"seq\":").append(seq);
    }

    /** {@code "sql":...,"params":[...]}, the params left out when empty. */
    private static void statement(StringBuilder sb, String sql, List<Object> params) throws SQLException {
        checkSql(sql);
        sb.append("\"sql\":");
        Values.appendString(sb, sql);
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
    }

    private static void checkSql(String sql) throws SQLException {
        if (sql == null || sql.isEmpty() || sql.indexOf('\0') >= 0) {
            throw new SQLNonTransientException("skylite: the SQL must be non-empty and hold no U+0000", "42000");
        }
    }

    private static StringBuilder start(String sql) throws SQLException {
        checkSql(sql);
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
