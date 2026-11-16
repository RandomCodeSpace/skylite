package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Columns;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Failure;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Kind;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Params;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Row;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FrameReaderTest {
    static final String COLS = "{\"columns\":[{\"name\":\"a\",\"decltype\":\"INTEGER\",\"database\":\"main\",\"table\":\"t\","
            + "\"origin\":\"a\"},{\"name\":\"b\",\"decltype\":null,\"database\":null,\"table\":null,\"origin\":null}]}";
    static final String DONE = FakeServer.done(7);

    static FrameReader reader(Kind kind, int maxFrame, String body) {
        return new FrameReader(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)), kind, maxFrame);
    }

    static List<Frame> readAll(Kind kind, String body) throws Exception {
        FrameReader r = reader(kind, FrameReader.MAX_FRAME, body);
        List<Frame> out = new ArrayList<>();
        Frame f;
        while ((f = r.next()) != null) {
            out.add(f);
        }
        return out;
    }

    static void refused(Kind kind, String body) {
        assertThrows(Values.Malformed.class, () -> readAll(kind, body));
    }

    @Test
    void readsAQueryAnswerSkippingKeepalivesUnknownFramesAndMembers() throws Exception {
        List<Frame> fs = readAll(Kind.QUERY, "{\"keepalive\":{}}\n" + COLS + "\n[1,\"x\"]\n{\"future\":[1,2]}\n"
                + "[{\"blob\":\"AAE=\"},null]\n{\"keepalive\":{}}\n"
                + "{\"done\":{\"txn\":7,\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":-3,\"new\":{\"x\":1}}}\n");
        assertEquals(4, fs.size());
        Columns c = assertInstanceOf(Columns.class, fs.get(0));
        assertEquals("a", c.columns().get(0).name());
        assertEquals("INTEGER", c.columns().get(0).declType());
        assertNull(c.columns().get(1).declType());
        assertArrayEquals(new Object[] {1L, "x"}, assertInstanceOf(Row.class, fs.get(1)).values());
        assertArrayEquals(new byte[] {0, 1}, (byte[]) assertInstanceOf(Row.class, fs.get(2)).values()[0]);
        Done d = assertInstanceOf(Done.class, fs.get(3));
        assertEquals(7, d.txn());
        assertEquals(-3, d.lastInsertRowid());
    }

    @Test
    void readsADescribeAnswer() throws Exception {
        List<Frame> fs = readAll(Kind.DESCRIBE, COLS + "\n{\"params\":{\"from\":0,\"names\":[\":t\",null]}}\n"
                + "{\"params\":{\"from\":2,\"names\":[\"@u\"]}}\n" + FakeServer.describeDone(3, 3) + "\n");
        assertEquals(java.util.Arrays.asList(":t", null), ((Params) fs.get(1)).names());
        assertEquals(3, ((Done) fs.get(3)).paramCount());
    }

    @Test
    void readsAnErrorFrame() throws Exception {
        Failure f = assertInstanceOf(Failure.class, readAll(Kind.QUERY, "{\"error\":{\"code\":\"sql_error\","
                + "\"not_executed\":true,\"final\":false,\"message\":\"no such table: t\",\"request_id\":\"r\","
                + "\"sqlite_code\":1,\"sqlite_extended_code\":1,\"later\":true}}\n").get(0));
        assertEquals("sql_error", f.code());
        assertEquals(1, f.sqliteCode());
    }

    @Test
    void aBodyWithoutTerminalFrameIsIncomplete() {
        assertThrows(EOFException.class, () -> readAll(Kind.QUERY, COLS + "\n[1,2]\n"));
        assertThrows(EOFException.class, () -> readAll(Kind.QUERY, COLS + "\n[1,2]"));
        assertThrows(EOFException.class, () -> readAll(Kind.QUERY, ""));
    }

    @Test
    void aRowWiderThanItsColumnsIsRefusedBeforeTheExtraValueIsDecoded() {
        // The third value is not even valid; the width check comes first.
        Values.Malformed e = assertThrows(Values.Malformed.class,
                () -> readAll(Kind.QUERY, COLS + "\n[1,2,{\"nonsense\":1}]\n" + DONE + "\n"));
        assertTrue(e.getMessage().contains("more values"), e.getMessage());
        refused(Kind.QUERY, COLS + "\n[1]\n" + DONE + "\n");
    }

    @Test
    void aFrameAboveTheBoundIsRefusedBeforeItIsRead() {
        AtomicLong served = new AtomicLong();
        InputStream endless = new InputStream() {
            @Override
            public int read() {
                served.incrementAndGet();
                return '1';
            }
        };
        FrameReader r = new FrameReader(endless, Kind.QUERY, 1 << 16);
        Values.Malformed e = assertThrows(Values.Malformed.class, r::next);
        assertTrue(e.getMessage().contains("exceeds"), e.getMessage());
        assertTrue(served.get() < 1 << 18, "read " + served.get() + " bytes of an endless frame");
    }

    @Test
    void theFrameBoundCountsTheNewline() throws Exception {
        String row = "[1,2]"; // with its newline, 6 bytes
        String body = COLS + "\n" + row + "\n" + DONE + "\n";
        int bound = Math.max(COLS.length(), DONE.length()) + 1;
        FrameReader ok = reader(Kind.QUERY, bound, body);
        while (ok.next() != null) {
            // reads to the end
        }
        assertThrows(Values.Malformed.class, () -> {
            FrameReader r = reader(Kind.QUERY, COLS.length(), body);
            r.next();
        });
    }

    @Test
    void aTerminalFrameAbove4KiBIsRefused() throws Exception {
        String fill = "x".repeat(4096);
        refused(Kind.QUERY, "{\"done\":{\"txn\":1,\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":0,"
                + "\"pad\":\"" + fill + "\"}}\n");
        String exact = "{\"done\":{\"txn\":1,\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":0,\"pad\":\"\"}}";
        String pad = "y".repeat(FrameReader.MAX_TERMINAL - 1 - exact.length());
        String frame = exact.replace("\"pad\":\"\"", "\"pad\":\"" + pad + "\"");
        assertEquals(FrameReader.MAX_TERMINAL, frame.length() + 1);
        assertInstanceOf(Done.class, readAll(Kind.QUERY, frame + "\n").get(0));
        refused(Kind.QUERY, frame.replace("\"pad\":\"", "\"pad\":\"y") + "\n"); // one byte more
    }

    @Test
    void bytesAfterTheTerminalFrameAreRefused() {
        refused(Kind.QUERY, DONE + "\n{\"keepalive\":{}}\n");
        refused(Kind.QUERY, DONE + "\nx");
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "[1,2]\n", // a row before columns
        "{\"columns\":[]}\n{\"columns\":[]}\n", // two columns frames
        "{}\n", // no member
        "{\"done\":{},\"x\":1}\n", // two members
        "\n", // an empty frame
        "1\n", // not an object or row
        "{\"results\":{\"from\":0,\"entries\":[{}]}}\n", // a batch frame
        "{\"params\":{\"from\":0,\"names\":[null]}}\n", // params in a query answer
        "{\"done\":{\"txn\":0,\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":0}}\n", // txn below 1
        "{\"done\":{\"txn\":1,\"read_only\":true,\"rows_affected\":0}}\n", // a member missing
        "{\"done\":{\"txn\":1.0,\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":0}}\n", // not an integer
        "{\"done\":{\"txn\":9223372036854775808,\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":0}}\n",
        "{\"done\":{\"txn\":1,\"txn\":2,\"read_only\":true,\"rows_affected\":0,\"last_insert_rowid\":0}}\n", // duplicate
        "{\"error\":{\"code\":\"busy\",\"not_executed\":true,\"final\":true,\"message\":\"m\"}}\n", // no request_id
        "{\"error\":{\"code\":\"busy\",\"not_executed\":1,\"final\":true,\"message\":\"m\",\"request_id\":\"r\"}}\n",
        "{\"error\":{\"code\":\"\\ud800\",\"not_executed\":true,\"final\":true,\"message\":\"m\",\"request_id\":\"r\"}}\n",
        "{\"columns\":[{\"decltype\":\"INT\"}]}\n", // a column without a name
        "{\"columns\":[]} junk\n", // not one JSON value
    })
    void framesThatBreakTheProtocolAreRefused(String body) {
        refused(Kind.QUERY, body);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "{\"params\":{\"from\":0,\"names\":[null]}}\n", // params before columns
        "{\"columns\":[]}\n{\"params\":{\"from\":1,\"names\":[null]}}\n", // a gap
        "{\"columns\":[]}\n{\"params\":{\"from\":0,\"names\":[]}}\n", // no names
        "{\"columns\":[]}\n{\"params\":{\"from\":0,\"names\":[null]}}\n{\"done\":{\"txn\":1,\"param_count\":2}}\n",
        "{\"done\":{\"txn\":1,\"param_count\":0}}\n", // no columns frame
    })
    void describeAnswersThatBreakTheProtocolAreRefused(String body) {
        refused(Kind.DESCRIBE, body);
    }

    @Test
    void invalidUtf8InAFrameIsRefused() {
        byte[] body = (COLS + "\n[1,\"\u00e9\"]\n" + DONE + "\n").getBytes(StandardCharsets.UTF_8);
        for (int i = 0; i < body.length - 1; i++) {
            if (body[i] == (byte) 0xc3) { // é's lead byte: an overlong / with c0 af in its place
                body[i] = (byte) 0xc0;
                body[i + 1] = (byte) 0xaf;
            }
        }
        FrameReader r = new FrameReader(new ByteArrayInputStream(body), Kind.QUERY, FrameReader.MAX_FRAME);
        assertThrows(Values.Malformed.class, () -> {
            while (r.next() != null) {
                // reads until refused
            }
        });
    }

    @Test
    void aBodyCutByTheTransportReportsTheIoError() {
        InputStream failing = new InputStream() {
            int n;

            @Override
            public int read() throws IOException {
                if (n < COLS.length() + 1) {
                    return (COLS + "\n").charAt(n++);
                }
                throw new IOException("reset");
            }
        };
        FrameReader r = new FrameReader(failing, Kind.QUERY, FrameReader.MAX_FRAME);
        assertThrows(IOException.class, () -> {
            while (r.next() != null) {
                // reads until the error
            }
        });
    }
}
