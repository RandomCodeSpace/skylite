package io.github.randomcodespace.skylite.jdbc;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.core.JsonToken;
import tools.jackson.core.ObjectReadContext;
import tools.jackson.core.StreamReadConstraints;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.core.json.JsonFactory;

/**
 * The value codec of ADR 0005 decision 8. A decoded value is {@code null}, a {@link Long} (INTEGER), a {@link Double}
 * (REAL), a {@link String} (TEXT that is valid UTF-8), a {@link TextBytes} (TEXT that is not) or a {@code byte[]}
 * (BLOB). The encoder writes the canonical form byte for byte, with the codec's own escaping rule, not a library's.
 */
final class Values {
    private Values() {}

    /** TEXT whose bytes are not valid UTF-8, which a Java string cannot hold. */
    record TextBytes(byte[] bytes) {}

    /** A value or frame that breaks the protocol. Its message never holds a value. */
    static final class Malformed extends Exception {
        private static final long serialVersionUID = 1L;

        Malformed(String message) {
            super(message);
        }
    }

    /**
     * The parser factory: duplicate names refused, strings as long as the largest frame. Jackson itself refuses
     * leading zeros, NaN, raw control characters in strings and invalid UTF-8 start bytes; {@link #checkUtf8} and
     * {@link #checkString} cover the encoded surrogates, overlong forms and lone surrogate escapes it lets through.
     */
    static final JsonFactory JSON = JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxStringLength(Integer.MAX_VALUE)
                    .maxDocumentLength(-1)
                    .maxNestingDepth(16)
                    .build())
            .build();

    static JsonParser parser(byte[] b, int off, int len) {
        return JSON.createParser(ObjectReadContext.empty(), b, off, len);
    }

    // ---- decoding ----

    /** Reads the value at the parser's current token. */
    static Object read(JsonParser p) throws Malformed {
        JsonToken t = p.currentToken();
        if (t == null) {
            throw new Malformed("a value is missing");
        }
        switch (t) {
            case VALUE_NULL:
                return null;
            case VALUE_NUMBER_INT:
                JsonParser.NumberType nt = p.getNumberType();
                if (nt != JsonParser.NumberType.INT && nt != JsonParser.NumberType.LONG) {
                    throw new Malformed("an INTEGER outside the signed 64-bit range");
                }
                return p.getLongValue();
            case VALUE_NUMBER_FLOAT:
                double d = Double.parseDouble(p.getString()); // never through Jackson's own conversion
                if (Double.isInfinite(d)) {
                    throw new Malformed("a REAL that does not parse to a finite double");
                }
                return d;
            case VALUE_STRING:
                return checkString(p.getString());
            case START_OBJECT:
                return tagged(p);
            default:
                throw new Malformed("a value is not null, a number, a string or a tagged object");
        }
    }

    private static Object tagged(JsonParser p) throws Malformed {
        String tag = p.nextName();
        if (tag == null) {
            throw new Malformed("a value object has no member");
        }
        p.nextToken();
        Object v;
        switch (tag) {
            case "real":
                String s = p.currentToken() == JsonToken.VALUE_STRING ? p.getString() : "";
                if (s.equals("Infinity")) {
                    v = Double.POSITIVE_INFINITY;
                } else if (s.equals("-Infinity")) {
                    v = Double.NEGATIVE_INFINITY;
                } else {
                    throw new Malformed("a real tag holds neither \"Infinity\" nor \"-Infinity\"");
                }
                break;
            case "text_bytes":
                byte[] b = base64(p);
                v = validUtf8(b, 0, b.length) ? new String(b, StandardCharsets.UTF_8) : new TextBytes(b);
                break;
            case "blob":
                v = base64(p);
                break;
            default:
                throw new Malformed("an unknown value tag");
        }
        if (p.nextToken() != JsonToken.END_OBJECT) {
            throw new Malformed("a value object has more than one member");
        }
        return v;
    }

    /** Standard base64 with padding, no line breaks and zero unused bits: exactly what the encoder writes back. */
    private static byte[] base64(JsonParser p) throws Malformed {
        if (p.currentToken() != JsonToken.VALUE_STRING) {
            throw new Malformed("a base64 value is not a string");
        }
        String s = p.getString();
        byte[] b;
        try {
            b = Base64.getDecoder().decode(s);
        } catch (IllegalArgumentException e) {
            throw new Malformed("invalid base64");
        }
        if (!Base64.getEncoder().encodeToString(b).equals(s)) {
            throw new Malformed("base64 that is not canonical: missing padding or non-zero unused bits");
        }
        return b;
    }

    /** Refuses a string holding a lone surrogate, which a JSON escape can produce. */
    static String checkString(String s) throws Malformed {
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (Character.isHighSurrogate(c) && i + 1 < s.length() && Character.isLowSurrogate(s.charAt(i + 1))) {
                i++;
            } else if (Character.isSurrogate(c)) {
                throw new Malformed("a string holds a lone surrogate");
            }
        }
        return s;
    }

    static boolean validUtf8(byte[] b, int off, int len) {
        try {
            checkUtf8(b, off, len);
            return true;
        } catch (Malformed e) {
            return false;
        }
    }

    /** Strict UTF-8, as the JDK's decoder reads it: no overlong forms, no encoded surrogates. Bounded memory. */
    static void checkUtf8(byte[] b, int off, int len) throws Malformed {
        CharsetDecoder d = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        ByteBuffer in = ByteBuffer.wrap(b, off, len);
        CharBuffer out = CharBuffer.allocate(Math.min(len, 8192) + 2);
        while (true) {
            CoderResult r = d.decode(in, out, true);
            if (r.isError()) {
                throw new Malformed("invalid UTF-8");
            }
            if (r.isUnderflow()) {
                break;
            }
            out.clear();
        }
        out.clear();
        if (d.flush(out).isError()) {
            throw new Malformed("invalid UTF-8");
        }
    }

    /**
     * Decodes the exact bytes of one value, as a conformance vector holds them: nothing may precede or follow it,
     * white space included.
     */
    static Object decode(byte[] wire) throws Malformed {
        if (wire.length == 0 || isSpace(wire[0]) || isSpace(wire[wire.length - 1])) {
            throw new Malformed("a value with white space around it, or no value");
        }
        checkUtf8(wire, 0, wire.length);
        try (JsonParser p = parser(wire, 0, wire.length)) {
            p.nextToken();
            Object v = read(p);
            if (p.nextToken() != null) {
                throw new Malformed("bytes follow the value");
            }
            return v;
        } catch (JacksonException e) {
            throw new Malformed("not one JSON value: " + e.getOriginalMessage());
        }
    }

    private static boolean isSpace(byte c) {
        return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    // ---- encoding ----

    /**
     * Appends the canonical encoding of v, one of the decoded types. The caller has converted every other Java type
     * (see {@link SkylitePreparedStatement}) and refused NaN.
     */
    static void append(StringBuilder sb, Object v) {
        switch (v) {
            case null -> sb.append("null");
            case Long l -> sb.append(l.longValue());
            case Double d -> appendReal(sb, d);
            case String s -> appendString(sb, s);
            case byte[] b -> sb.append("{\"blob\":\"").append(Base64.getEncoder().encodeToString(b)).append("\"}");
            case TextBytes t -> {
                if (validUtf8(t.bytes(), 0, t.bytes().length)) {
                    appendString(sb, new String(t.bytes(), StandardCharsets.UTF_8));
                } else {
                    sb.append("{\"text_bytes\":\"").append(Base64.getEncoder().encodeToString(t.bytes())).append("\"}");
                }
            }
            default -> throw new IllegalArgumentException("no wire encoding for " + v.getClass().getName());
        }
    }

    /**
     * The canonical REAL of ADR 0005: the shortest digits that parse back, {@code [-]d[.ddd]e[-]N}. Double.toString is
     * shortest from JDK 19 on, except that where one digit would do it may give two; those are rounded half-even to
     * one digit, kept if that parses back to the same double.
     */
    static void appendReal(StringBuilder sb, double d) {
        if (Double.isNaN(d)) {
            throw new IllegalArgumentException("NaN has no wire encoding");
        }
        if (Double.isInfinite(d)) {
            sb.append(d > 0 ? "{\"real\":\"Infinity\"}" : "{\"real\":\"-Infinity\"}");
            return;
        }
        if (Double.doubleToRawLongBits(d) < 0) {
            sb.append('-');
        }
        double a = Math.abs(d);
        if (a == 0) {
            sb.append("0e0");
            return;
        }
        BigDecimal b = new BigDecimal(Double.toString(a)).stripTrailingZeros();
        if (b.precision() == 2) {
            BigDecimal one = b.round(new MathContext(1, RoundingMode.HALF_EVEN));
            if (Double.parseDouble(one.toString()) == a) {
                b = one.stripTrailingZeros();
            }
        }
        String digits = b.unscaledValue().toString();
        long exp = (long) digits.length() - 1 - b.scale();
        sb.append(digits.charAt(0));
        if (digits.length() > 1) {
            sb.append('.').append(digits, 1, digits.length());
        }
        sb.append('e').append(exp);
    }

    /** A JSON string with exactly ADR 0005's escapes. The caller has refused lone surrogates. */
    static void appendString(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\t' -> sb.append("\\t");
                case '\n' -> sb.append("\\n");
                case '\f' -> sb.append("\\f");
                case '\r' -> sb.append("\\r");
                default -> {
                    if (c < 0x20) {
                        sb.append("\\u00").append(Character.forDigit(c >> 4, 16)).append(Character.forDigit(c & 15, 16));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    /** UTF-8 bytes of a string, refusing lone surrogates, which UTF-8 cannot carry. */
    static byte[] utf8(String s) throws CharacterCodingException {
        ByteBuffer b = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .encode(CharBuffer.wrap(s));
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }
}
