package io.github.randomcodespace.skylite.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.randomcodespace.skylite.jdbc.Values.TextBytes;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;

/**
 * The value codec against clients/conformance/values.ndjson, as its README says: every valid wire decodes to its class
 * and value, every valid vector without decode_only encodes back to the identical bytes, and every vector with an
 * error is refused.
 */
class ConformanceTest {
    static final Path VECTORS = Path.of("../../conformance/values.ndjson");

    record Vector(String name, byte[] wire, String cls, String value, boolean decodeOnly, String error) {}

    static List<Vector> vectors() throws Exception {
        List<Vector> out = new ArrayList<>();
        for (String line : Files.readAllLines(VECTORS, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            String name = null, wire = null, wireHex = null, cls = null, value = null, error = null;
            boolean decodeOnly = false;
            int repeat = 1;
            byte[] b = line.getBytes(StandardCharsets.UTF_8);
            try (JsonParser p = Values.parser(b, 0, b.length)) {
                p.nextToken();
                String m;
                while ((m = p.nextName()) != null) {
                    JsonToken t = p.nextToken();
                    switch (m) {
                        case "name" -> name = p.getString();
                        case "wire" -> wire = p.getString();
                        case "wire_hex" -> wireHex = p.getString();
                        case "class" -> cls = p.getString();
                        case "value" -> value = t == JsonToken.VALUE_NULL ? null : p.getString();
                        case "decode_only" -> decodeOnly = t == JsonToken.VALUE_TRUE;
                        case "error" -> error = p.getString();
                        case "repeat" -> repeat = p.getIntValue();
                        default -> throw new AssertionError("unknown vector member " + m);
                    }
                }
            }
            byte[] bytes;
            if (wireHex != null) {
                bytes = HexFormat.of().parseHex(wireHex);
            } else if (repeat > 1) {
                // A string wire "x" stands for the string of x repeated: the quotes once, the contents repeated.
                String inner = wire.substring(1, wire.length() - 1);
                bytes = ("\"" + inner.repeat(repeat) + "\"").getBytes(StandardCharsets.UTF_8);
                value = value.repeat(repeat);
            } else {
                bytes = wire.getBytes(StandardCharsets.UTF_8);
            }
            out.add(new Vector(name, bytes, cls, value, decodeOnly, error));
        }
        return out;
    }

    @Test
    void vectorsAreLoaded() throws Exception {
        assertTrue(vectors().size() > 100, "values.ndjson has its vectors");
    }

    @Test
    void aByteOrderMarkIsRefused() {
        assertThrows(Values.Malformed.class, () -> Values.decode(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF, '1'}));
    }

    @TestFactory
    Stream<DynamicTest> decode() throws Exception {
        return vectors().stream().map(v -> DynamicTest.dynamicTest(v.name(), () -> {
            if (v.error() != null) {
                assertThrows(Values.Malformed.class, () -> Values.decode(v.wire()));
                return;
            }
            Object got = Values.decode(v.wire());
            assertEquals(v.cls(), className(got));
            assertEquals(v.value(), canonicalValue(got));
        }));
    }

    @TestFactory
    Stream<DynamicTest> encode() throws Exception {
        return vectors().stream()
                .filter(v -> v.error() == null && !v.decodeOnly())
                .map(v -> DynamicTest.dynamicTest(v.name(), () -> {
                    StringBuilder sb = new StringBuilder();
                    Values.append(sb, javaValue(v.cls(), v.value()));
                    assertArrayEquals(v.wire(), Values.utf8(sb.toString()));
                }));
    }

    static String className(Object v) {
        return switch (v) {
            case null -> "null";
            case Long l -> "integer";
            case Double d -> "real";
            case String s -> "text";
            case TextBytes t -> "text";
            case byte[] b -> "blob";
            default -> throw new AssertionError(v.getClass());
        };
    }

    static String canonicalValue(Object v) {
        return switch (v) {
            case null -> null;
            case Long l -> l.toString();
            case Double d -> String.format("%016x", Double.doubleToRawLongBits(d));
            case String s -> HexFormat.of().formatHex(s.getBytes(StandardCharsets.UTF_8));
            case TextBytes t -> HexFormat.of().formatHex(t.bytes());
            case byte[] b -> HexFormat.of().formatHex(b);
            default -> throw new AssertionError(v.getClass());
        };
    }

    static Object javaValue(String cls, String value) {
        return switch (cls) {
            case "null" -> null;
            case "integer" -> Long.parseLong(value);
            case "real" -> Double.longBitsToDouble(Long.parseUnsignedLong(value, 16));
            case "text" -> {
                byte[] b = HexFormat.of().parseHex(value);
                yield Values.validUtf8(b, 0, b.length) ? new String(b, StandardCharsets.UTF_8) : new TextBytes(b);
            }
            case "blob" -> HexFormat.of().parseHex(value);
            default -> throw new AssertionError(cls);
        };
    }
}
