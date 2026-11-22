package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.Values.Malformed;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;

/**
 * Reads a response body of ADR 0005 one NDJSON frame at a time, as it arrives, keeping at most one frame in memory.
 * It checks the frame bound before parsing, strict UTF-8, the order of the grammar, each row's width before decoding
 * a value it would drop, the continuity of params entries, the members of the terminal frame the request expects, the
 * terminal frame's 4 KiB bound, and that nothing follows the terminal frame. Keepalives, frames whose only member is
 * unknown, and unknown members of known objects are skipped. Mirrors internal/wire's Reader.
 */
final class FrameReader {
    /** The largest frame read, newline included: a row may be as large as the default result limit, 64 MiB. */
    static final int MAX_FRAME = (64 << 20) + 1;
    /** ADR 0005 reserves 4 KiB for the terminal frame, newline included; no configuration makes one larger. */
    static final int MAX_TERMINAL = 4 << 10;
    /** ADR 0005's frame size, newline included: a columns frame is never larger. */
    static final int MAX_COLUMNS_FRAME = 1 << 20;
    /** The most entries in one params frame. */
    private static final int MAX_FRAME_ENTRIES = 1024;

    enum Kind { QUERY, DESCRIBE }

    record Column(String name, String declType, String database, String table, String origin) {}

    sealed interface Frame permits Columns, Row, Params, Done, Failure {}

    record Columns(List<Column> columns) implements Frame {}

    record Row(Object[] values) implements Frame {}

    record Params(List<String> names) implements Frame {}

    /** A done frame; fields the request kind does not carry are 0 or false. */
    record Done(long txn, boolean readOnly, long rowsAffected, long lastInsertRowid, long paramCount) implements Frame {}

    /** An error frame. Optional integers are -1 when absent. */
    record Failure(
            String code,
            boolean notExecuted,
            boolean isFinal,
            String message,
            String requestId,
            long sqliteCode,
            long sqliteExtendedCode,
            String cause,
            long txn,
            long epoch)
            implements Frame {}

    private enum State { START, ROWS, PARAMS, ENDED }

    private final InputStream in;
    private final Kind kind;
    private final int maxFrame;
    private final byte[] buf = new byte[64 << 10];
    private int pos, lim;
    private byte[] line = new byte[256];
    private int lineLen;
    private State state = State.START;
    private int columns;
    private long params;

    FrameReader(InputStream in, Kind kind, int maxFrame) {
        this.in = in;
        this.kind = kind;
        this.maxFrame = maxFrame;
    }

    /**
     * Returns the next frame, or null after the terminal frame. A body that ends before its terminal frame throws
     * {@link EOFException}; one that breaks the protocol throws {@link Malformed}.
     */
    Frame next() throws IOException, Malformed {
        while (state != State.ENDED) {
            readLine();
            Values.checkUtf8(line, 0, lineLen);
            Values.checkEscapes(line, 0, lineLen);
            Frame f;
            try (JsonParser p = Values.parser(line, 0, lineLen)) {
                f = frame(p);
                if (p.nextToken() != null) {
                    throw new Malformed("a frame is not one JSON value");
                }
            } catch (JacksonException e) {
                throw new Malformed("a frame is not valid JSON: " + e.getOriginalMessage());
            }
            if (f == null) {
                continue; // skipped
            }
            if (f instanceof Done || f instanceof Failure) {
                state = State.ENDED;
                if (pos < lim || fill() > 0) {
                    throw new Malformed("bytes follow the terminal frame");
                }
            }
            return f;
        }
        return null;
    }

    private int fill() throws IOException {
        int n = in.read(buf);
        pos = 0;
        lim = Math.max(n, 0);
        return n;
    }

    /** Reads one frame into line, without its newline, refusing one above the bound before it is all read. */
    private void readLine() throws IOException, Malformed {
        lineLen = 0;
        while (true) {
            if (pos == lim && fill() < 0) {
                throw new EOFException("the response ended before its terminal frame");
            }
            int nl = -1;
            for (int i = pos; i < lim; i++) {
                if (buf[i] == '\n') {
                    nl = i;
                    break;
                }
            }
            int end = nl < 0 ? lim : nl + 1;
            int n = end - pos;
            if ((long) lineLen + n > maxFrame) {
                throw new Malformed("a frame exceeds " + maxFrame + " bytes");
            }
            if (lineLen + n > line.length) {
                line = Arrays.copyOf(line, (int) Math.min(maxFrame, Math.max((long) line.length * 2, lineLen + n)));
            }
            System.arraycopy(buf, pos, line, lineLen, n);
            lineLen += n;
            pos = end;
            if (nl >= 0) {
                lineLen--; // the newline, still counted against the bound above
                if (lineLen == 0) {
                    throw new Malformed("an empty frame");
                }
                return;
            }
        }
    }

    private Frame frame(JsonParser p) throws Malformed {
        byte last = line[lineLen - 1];
        if (last != '}' && last != ']') {
            throw new Malformed("bytes follow a frame's JSON value");
        }
        if (line[0] == '[') {
            if (state != State.ROWS || kind == Kind.DESCRIBE) {
                throw new Malformed("a row outside the rows of a result");
            }
            return row(p);
        }
        if (line[0] != '{') {
            throw new Malformed("a frame must be an object or a row");
        }
        p.nextToken();
        String name = p.nextName();
        if (name == null) {
            throw new Malformed("a frame object has no member");
        }
        p.nextToken();
        Frame f;
        switch (name) {
            case "keepalive" -> {
                p.skipChildren();
                f = null;
            }
            case "columns" -> {
                if (state != State.START) {
                    throw new Malformed("a columns frame after other frames");
                }
                if (lineLen + 1 > MAX_COLUMNS_FRAME) {
                    throw new Malformed("a columns frame exceeds " + MAX_COLUMNS_FRAME + " bytes");
                }
                List<Column> cols = columnsFrame(p);
                state = State.ROWS;
                columns = cols.size();
                f = new Columns(cols);
            }
            case "results" -> throw new Malformed("a results frame in this response");
            case "params" -> {
                if (kind != Kind.DESCRIBE || (state != State.ROWS && state != State.PARAMS)) {
                    throw new Malformed("a params frame in this response or before its columns");
                }
                f = paramsFrame(p);
                state = State.PARAMS;
            }
            case "done", "error" -> {
                if (lineLen + 1 > MAX_TERMINAL) {
                    throw new Malformed("a terminal frame exceeds " + MAX_TERMINAL + " bytes");
                }
                f = name.equals("done") ? done(p) : failure(p);
            }
            default -> {
                p.skipChildren();
                f = null;
            }
        }
        if (p.nextToken() != JsonToken.END_OBJECT) {
            throw new Malformed("a frame object has more than one member");
        }
        return f;
    }

    private Row row(JsonParser p) throws Malformed {
        p.nextToken(); // [
        Object[] vals = new Object[columns];
        int n = 0;
        while (p.nextToken() != JsonToken.END_ARRAY) {
            if (n == columns) { // before decoding a value it would drop
                throw new Malformed("a row has more values than its " + columns + " columns");
            }
            vals[n++] = Values.read(p);
        }
        if (n != columns) {
            throw new Malformed("a row has " + n + " values for " + columns + " columns");
        }
        return new Row(vals);
    }

    private List<Column> columnsFrame(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_ARRAY) {
            throw new Malformed("columns is not an array");
        }
        List<Column> cols = new ArrayList<>();
        while (p.nextToken() != JsonToken.END_ARRAY) {
            if (p.currentToken() != JsonToken.START_OBJECT) {
                throw new Malformed("a column is not an object");
            }
            String colName = null, decl = null, db = null, table = null, origin = null;
            String m;
            while ((m = p.nextName()) != null) {
                p.nextToken();
                switch (m) {
                    case "name" -> colName = str(p, m);
                    case "decltype" -> decl = nullStr(p, m);
                    case "database" -> db = nullStr(p, m);
                    case "table" -> table = nullStr(p, m);
                    case "origin" -> origin = nullStr(p, m);
                    default -> p.skipChildren();
                }
            }
            if (colName == null) {
                throw new Malformed("a column without a name");
            }
            cols.add(new Column(colName, decl, db, table, origin));
        }
        return cols;
    }

    private Params paramsFrame(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new Malformed("params is not an object");
        }
        long from = -1;
        List<String> names = null;
        String m;
        while ((m = p.nextName()) != null) {
            p.nextToken();
            switch (m) {
                case "from" -> from = integer(p, m, 0, Long.MAX_VALUE);
                case "names" -> {
                    if (p.currentToken() != JsonToken.START_ARRAY) {
                        throw new Malformed("names is not an array");
                    }
                    names = new ArrayList<>();
                    while (p.nextToken() != JsonToken.END_ARRAY) {
                        if (names.size() == MAX_FRAME_ENTRIES) { // before reading an entry it would refuse
                            throw new Malformed("a params frame holds more than " + MAX_FRAME_ENTRIES + " names");
                        }
                        names.add(nullStr(p, "a parameter name"));
                    }
                }
                default -> p.skipChildren();
            }
        }
        if (from != params) {
            throw new Malformed("params entries from " + from + " where " + params + " was next");
        }
        if (names == null || names.isEmpty() || names.size() > MAX_FRAME_ENTRIES) {
            throw new Malformed("a params frame holds 1 to " + MAX_FRAME_ENTRIES + " names");
        }
        params += names.size();
        return new Params(names);
    }

    private Done done(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new Malformed("done is not an object");
        }
        long txn = -1, rows = -1, paramCount = -1;
        long lastId = 0;
        boolean lastIdSeen = false;
        Boolean readOnly = null;
        String m;
        while ((m = p.nextName()) != null) {
            p.nextToken();
            switch (m) {
                case "txn" -> txn = integer(p, m, 1, Long.MAX_VALUE);
                case "read_only" -> readOnly = bool(p, m);
                case "rows_affected" -> rows = integer(p, m, 0, Long.MAX_VALUE);
                case "last_insert_rowid" -> {
                    lastId = integer(p, m, Long.MIN_VALUE, Long.MAX_VALUE);
                    lastIdSeen = true;
                }
                case "param_count" -> paramCount = integer(p, m, 0, Integer.MAX_VALUE);
                default -> p.skipChildren();
            }
        }
        boolean complete = txn > 0 && switch (kind) {
            case QUERY -> readOnly != null && rows >= 0 && lastIdSeen;
            case DESCRIBE -> paramCount >= 0;
        };
        if (!complete) {
            throw new Malformed("the done frame lacks a member its request's answer carries");
        }
        if (kind == Kind.DESCRIBE) {
            if (state == State.START) {
                throw new Malformed("a describe answer without its columns frame");
            }
            if (params != paramCount) {
                throw new Malformed(params + " parameter names for param_count " + paramCount);
            }
        }
        return new Done(txn, readOnly != null && readOnly, Math.max(rows, 0), lastId, Math.max(paramCount, 0));
    }

    private Failure failure(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new Malformed("error is not an object");
        }
        String code = null, message = null, requestId = null, cause = null;
        Boolean notExecuted = null, fin = null;
        long sqlite = -1, extended = -1, txn = -1, epoch = -1;
        String m;
        while ((m = p.nextName()) != null) {
            p.nextToken();
            switch (m) {
                case "code" -> code = str(p, m);
                case "not_executed" -> notExecuted = bool(p, m);
                case "final" -> fin = bool(p, m);
                case "message" -> message = str(p, m);
                case "request_id" -> requestId = str(p, m);
                case "channel_id", "txn_state" -> str(p, m);
                case "seq" -> integer(p, m, 1, Long.MAX_VALUE);
                case "sqlite_code" -> sqlite = integer(p, m, 0, 255);
                case "sqlite_extended_code" -> extended = integer(p, m, 0, Integer.MAX_VALUE);
                case "batch_index" -> integer(p, m, 0, 9999);
                case "cause" -> cause = str(p, m);
                case "txn" -> txn = integer(p, m, 1, Long.MAX_VALUE);
                case "epoch" -> epoch = integer(p, m, 1, Long.MAX_VALUE);
                default -> p.skipChildren();
            }
        }
        if (code == null || notExecuted == null || fin == null || message == null || requestId == null) {
            throw new Malformed("the error lacks code, not_executed, final, message or request_id");
        }
        return new Failure(code, notExecuted, fin, message, requestId, sqlite, extended, cause, txn, epoch);
    }

    private static String str(JsonParser p, String what) throws Malformed {
        if (p.currentToken() != JsonToken.VALUE_STRING) {
            throw new Malformed("member " + what + " is not a string");
        }
        return Values.checkString(p.getString());
    }

    private static String nullStr(JsonParser p, String what) throws Malformed {
        return p.currentToken() == JsonToken.VALUE_NULL ? null : str(p, what);
    }

    private static boolean bool(JsonParser p, String what) throws Malformed {
        JsonToken t = p.currentToken();
        if (t != JsonToken.VALUE_TRUE && t != JsonToken.VALUE_FALSE) {
            throw new Malformed("member " + what + " is not a boolean");
        }
        return t == JsonToken.VALUE_TRUE;
    }

    /** An integer in [lo, hi], read exactly, never through a double. */
    private static long integer(JsonParser p, String what, long lo, long hi) throws Malformed {
        if (p.currentToken() == JsonToken.VALUE_NUMBER_INT) {
            JsonParser.NumberType nt = p.getNumberType();
            if (nt == JsonParser.NumberType.INT || nt == JsonParser.NumberType.LONG) {
                long v = p.getLongValue();
                if (v >= lo && v <= hi) {
                    return v;
                }
            }
        }
        throw new Malformed("member " + what + " has the wrong type or range");
    }
}
