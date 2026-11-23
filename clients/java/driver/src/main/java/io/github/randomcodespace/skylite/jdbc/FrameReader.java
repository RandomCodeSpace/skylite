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
 *
 * <p>The kinds are the answers the driver reads: a stateless query or describe; on a write channel an execute (a
 * statement, alone or as a begin's first), a batch, a step (begin, commit or rollback), the open, cancel and close
 * answers, and a resolve's.
 */
final class FrameReader {
    /** The largest frame read, newline included: a row may be as large as the default result limit, 64 MiB. */
    static final int MAX_FRAME = (64 << 20) + 1;
    /** ADR 0005 reserves 4 KiB for the terminal frame, newline included; no configuration makes one larger. */
    static final int MAX_TERMINAL = 4 << 10;
    /** The most entries in one params frame. */
    private static final int MAX_FRAME_ENTRIES = 1024;

    enum Kind { QUERY, DESCRIBE, EXECUTE, BATCH, STEP, OPEN, CANCEL, CLOSE, RESOLVE }

    /** Frames to read in order: a response as it arrives, or one read in full. */
    interface Frames extends AutoCloseable {
        /** The next frame, or null after the terminal frame. */
        Frame next() throws java.sql.SQLException;

        @Override
        void close();

        /** The frames of an answer read in full. */
        static Frames of(List<Frame> frames) {
            java.util.Iterator<Frame> it = frames.iterator();
            return new Frames() {
                @Override
                public Frame next() {
                    return it.hasNext() ? it.next() : null;
                }

                @Override
                public void close() {}
            };
        }
    }

    record Column(String name, String declType, String database, String table, String origin) {}

    sealed interface Frame permits Columns, Row, Params, Results, Done, Failure {}

    record Columns(List<Column> columns) implements Frame {}

    record Row(Object[] values) implements Frame {}

    record Params(List<String> names) implements Frame {}

    /** A batch's results frame: each entry's rows_affected and last_insert_rowid, in order. */
    record Results(long[] rowsAffected, long[] lastInsertRowids) implements Frame {}

    /**
     * The limits of an open answer that the driver acts on (ADR 0005, the limits object); a member left out keeps
     * the protocol's default.
     */
    record Limits(long requestBodyBytes, long batchEntries, long statementMs, long resolveBound) {
        static final Limits DEFAULT = new Limits(16 << 20, 10_000, 30_000, 100_000);
    }

    /**
     * A done frame, or a successful receipt; fields the request kind does not carry are 0, false or null. token and
     * limits are an open answer's; lastSeq a close answer's; outcome a cancel or close answer's receipt, a Done or a
     * Failure; entries a batch's; resolution a resolve's.
     */
    record Done(long txn, boolean readOnly, long rowsAffected, long lastInsertRowid, long paramCount, long seq,
            String txnState, String token, long lastSeq, Frame outcome, boolean dataOmitted, long entries,
            String resolution, Limits limits) implements Frame {
        /** The same answer, marked as one whose data the driver does not have. */
        Done omitted() {
            return new Done(txn, readOnly, rowsAffected, lastInsertRowid, paramCount, seq, txnState, token, lastSeq,
                    outcome, true, entries, resolution, limits);
        }
    }

    /**
     * An error frame, or a failed receipt. Optional integers are -1 when absent, txnState null; batchIndex is a
     * failed batch entry's index.
     */
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
            long epoch,
            String txnState,
            long batchIndex)
            implements Frame {}

    private enum State { START, ROWS, PARAMS, RESULTS, ENDED }

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
    private long results;

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
            if (state != State.ROWS || (kind != Kind.QUERY && kind != Kind.EXECUTE)) {
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
                if (state != State.START || (kind != Kind.QUERY && kind != Kind.EXECUTE && kind != Kind.DESCRIBE)) {
                    throw new Malformed("a columns frame after other frames, or in an answer without columns");
                }
                List<Column> cols = columnsFrame(p);
                state = State.ROWS;
                columns = cols.size();
                f = new Columns(cols);
            }
            case "results" -> {
                if (kind != Kind.BATCH) {
                    throw new Malformed("a results frame in this response");
                }
                f = resultsFrame(p);
                state = State.RESULTS;
            }
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

    private Results resultsFrame(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new Malformed("results is not an object");
        }
        long from = -1;
        List<long[]> entries = null;
        String m;
        while ((m = p.nextName()) != null) {
            p.nextToken();
            switch (m) {
                case "from" -> from = integer(p, m, 0, Long.MAX_VALUE);
                case "entries" -> {
                    if (p.currentToken() != JsonToken.START_ARRAY) {
                        throw new Malformed("entries is not an array");
                    }
                    entries = new ArrayList<>();
                    while (p.nextToken() != JsonToken.END_ARRAY) {
                        if (entries.size() == MAX_FRAME_ENTRIES || p.currentToken() != JsonToken.START_OBJECT) {
                            throw new Malformed("a results frame holds 1 to " + MAX_FRAME_ENTRIES + " objects");
                        }
                        long rows = -1, rowid = 0;
                        boolean seen = false;
                        String e;
                        while ((e = p.nextName()) != null) {
                            p.nextToken();
                            switch (e) {
                                case "rows_affected" -> rows = integer(p, e, 0, Long.MAX_VALUE);
                                case "last_insert_rowid" -> {
                                    rowid = integer(p, e, Long.MIN_VALUE, Long.MAX_VALUE);
                                    seen = true;
                                }
                                default -> p.skipChildren();
                            }
                        }
                        if (rows < 0 || !seen) {
                            throw new Malformed("a results entry lacks rows_affected or last_insert_rowid");
                        }
                        entries.add(new long[] {rows, rowid});
                    }
                }
                default -> p.skipChildren();
            }
        }
        if (from != results) {
            throw new Malformed("results entries from " + from + " where " + results + " was next");
        }
        if (entries == null || entries.isEmpty()) {
            throw new Malformed("a results frame holds 1 to " + MAX_FRAME_ENTRIES + " entries");
        }
        results += entries.size();
        long[] rows = new long[entries.size()], ids = new long[entries.size()];
        for (int i = 0; i < rows.length; i++) {
            rows[i] = entries.get(i)[0];
            ids[i] = entries.get(i)[1];
        }
        return new Results(rows, ids);
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

    /** The members of a done object or a receipt, as read; absent integers are -1. */
    private static final class Fields {
        long txn = -1, rows = -1, paramCount = -1, seq = -1, lastSeq = -1, entries = -1;
        long lastId;
        boolean lastIdSeen, dataOmitted, hasChannel;
        Boolean readOnly, ok;
        String txnState, token, resolution;
        Frame outcome;
        Limits limits;
    }

    private static Fields fields(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new Malformed("done is not an object");
        }
        Fields d = new Fields();
        String m;
        while ((m = p.nextName()) != null) {
            p.nextToken();
            switch (m) {
                case "txn" -> d.txn = integer(p, m, 1, Long.MAX_VALUE);
                case "read_only" -> d.readOnly = bool(p, m);
                case "rows_affected" -> d.rows = integer(p, m, 0, Long.MAX_VALUE);
                case "last_insert_rowid" -> {
                    d.lastId = integer(p, m, Long.MIN_VALUE, Long.MAX_VALUE);
                    d.lastIdSeen = true;
                }
                case "param_count" -> d.paramCount = integer(p, m, 0, Integer.MAX_VALUE);
                case "seq" -> d.seq = integer(p, m, 1, Long.MAX_VALUE);
                case "last_seq" -> d.lastSeq = integer(p, m, 0, Long.MAX_VALUE);
                case "txn_state" -> d.txnState = txnState(p);
                case "data_omitted" -> d.dataOmitted = bool(p, m);
                case "entries" -> d.entries = integer(p, m, 1, 10_000);
                case "resolution" -> {
                    d.resolution = str(p, m);
                    if (!List.of("committed", "not_committed", "pending", "unknown").contains(d.resolution)) {
                        throw new Malformed("resolution is not committed, not_committed, pending or unknown");
                    }
                }
                case "ok" -> d.ok = bool(p, m);
                case "error" -> d.outcome = failure(p);
                case "outcome" -> d.outcome = receipt(p);
                case "channel" -> {
                    if (p.currentToken() != JsonToken.START_OBJECT) {
                        throw new Malformed("channel is not an object");
                    }
                    d.hasChannel = true;
                    String c;
                    while ((c = p.nextName()) != null) {
                        p.nextToken();
                        if (c.equals("token")) {
                            d.token = str(p, c);
                        } else if (c.equals("limits")) {
                            d.limits = limits(p);
                        } else {
                            p.skipChildren();
                        }
                    }
                }
                default -> p.skipChildren();
            }
        }
        return d;
    }

    /** The limits object: the members the driver acts on, each 1 or more. */
    private static Limits limits(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new Malformed("limits is not an object");
        }
        Limits l = Limits.DEFAULT;
        long body = l.requestBodyBytes(), batch = l.batchEntries(), stmt = l.statementMs(), bound = l.resolveBound();
        String m;
        while ((m = p.nextName()) != null) {
            p.nextToken();
            switch (m) {
                case "request_body_bytes" -> body = integer(p, m, 1, Long.MAX_VALUE);
                case "batch_entries" -> batch = integer(p, m, 1, 10_000);
                case "statement_ms" -> stmt = integer(p, m, 1, Integer.MAX_VALUE);
                case "resolve_bound" -> bound = integer(p, m, 1, Long.MAX_VALUE);
                default -> p.skipChildren();
            }
        }
        return new Limits(body, batch, stmt, bound);
    }

    private static Done done(Fields d) {
        return new Done(Math.max(d.txn, 0), d.readOnly != null && d.readOnly, Math.max(d.rows, 0), d.lastId,
                Math.max(d.paramCount, 0), Math.max(d.seq, 0), d.txnState, d.token, Math.max(d.lastSeq, 0), d.outcome,
                d.dataOmitted, Math.max(d.entries, 0), d.resolution, d.limits != null ? d.limits : Limits.DEFAULT);
    }

    /** A receipt, as a cancel or close answer embeds it: a Done when ok, else its Failure. */
    private static Frame receipt(JsonParser p) throws Malformed {
        Fields d = fields(p);
        if (d.ok == null || d.seq < 1 || (d.ok ? d.outcome != null || d.txnState == null : !(d.outcome instanceof Failure))) {
            throw new Malformed("a receipt lacks seq, ok, txn_state or its error");
        }
        return d.ok ? done(d) : d.outcome;
    }

    private Done done(JsonParser p) throws Malformed {
        Fields d = fields(p);
        boolean query = d.readOnly != null && d.rows >= 0 && d.lastIdSeen;
        boolean step = d.seq > 0 && d.txnState != null && d.txn > 0;
        boolean complete = d.ok == null && switch (kind) {
            case QUERY -> d.txn > 0 && query;
            case DESCRIBE -> d.txn > 0 && d.paramCount >= 0;
            case EXECUTE -> step && query;
            case BATCH -> step && d.rows >= 0 && d.lastIdSeen && d.entries > 0;
            case RESOLVE -> d.txn > 0 && d.resolution != null;
            case STEP -> step;
            case OPEN -> d.txn > 0 && d.token != null;
            case CANCEL -> d.seq > 0 && d.txnState != null && d.outcome != null;
            case CLOSE -> d.lastSeq >= 0 && (d.lastSeq == 0) == (d.outcome == null);
        };
        if (!complete || (d.outcome != null && kind != Kind.CANCEL && kind != Kind.CLOSE)) {
            throw new Malformed("the done frame lacks a member its request's answer carries");
        }
        if (kind == Kind.BATCH && results != (d.dataOmitted ? 0 : d.entries)) { // a replay without its data has none
            throw new Malformed(results + " results entries for " + d.entries + " batch entries");
        }
        if (kind == Kind.DESCRIBE) {
            if (state == State.START) {
                throw new Malformed("a describe answer without its columns frame");
            }
            if (params != d.paramCount) {
                throw new Malformed(params + " parameter names for param_count " + d.paramCount);
            }
        }
        return done(d);
    }

    private static Failure failure(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.START_OBJECT) {
            throw new Malformed("error is not an object");
        }
        String code = null, message = null, requestId = null, cause = null, txnState = null;
        Boolean notExecuted = null, fin = null;
        long sqlite = -1, extended = -1, txn = -1, epoch = -1, batchIndex = -1;
        String m;
        while ((m = p.nextName()) != null) {
            p.nextToken();
            switch (m) {
                case "code" -> code = str(p, m);
                case "not_executed" -> notExecuted = bool(p, m);
                case "final" -> fin = bool(p, m);
                case "message" -> message = str(p, m);
                case "request_id" -> requestId = str(p, m);
                case "channel_id" -> str(p, m);
                case "txn_state" -> txnState = txnState(p);
                case "seq" -> integer(p, m, 1, Long.MAX_VALUE);
                case "sqlite_code" -> sqlite = integer(p, m, 0, 255);
                case "sqlite_extended_code" -> extended = integer(p, m, 0, Integer.MAX_VALUE);
                case "batch_index" -> batchIndex = integer(p, m, 0, 9999);
                case "cause" -> cause = str(p, m);
                case "txn" -> txn = integer(p, m, 1, Long.MAX_VALUE);
                case "epoch" -> epoch = integer(p, m, 1, Long.MAX_VALUE);
                default -> p.skipChildren();
            }
        }
        if (code == null || notExecuted == null || fin == null || message == null || requestId == null) {
            throw new Malformed("the error lacks code, not_executed, final, message or request_id");
        }
        return new Failure(code, notExecuted, fin, message, requestId, sqlite, extended, cause, txn, epoch, txnState,
                batchIndex);
    }

    private static String txnState(JsonParser p) throws Malformed {
        String t = str(p, "txn_state");
        if (!t.equals("none") && !t.equals("open") && !t.equals("aborted")) {
            throw new Malformed("txn_state is not none, open or aborted");
        }
        return t;
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
