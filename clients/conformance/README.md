# Conformance vectors

`values.ndjson` holds the value vectors of [ADR 0005](../../docs/content/docs/decisions/0005-client-protocol.md#8-values-and-shared-conventions). The Go codec tests against it in `internal/wire`. The JDBC codec will test against it too.

Each line is one JSON object with these members:

| Member | Meaning |
|---|---|
| `name` | A unique name for the vector. |
| `wire` | The value's exact bytes on the wire, as a JSON string. |
| `wire_hex` | Replaces `wire` when those bytes are not valid UTF-8, which a JSON string cannot hold: the bytes in hexadecimal. |
| `class` | The storage class: `null`, `integer`, `real`, `text` or `blob`. |
| `value` | The value, as text: decimal for an INTEGER; the 16 hexadecimal digits of the IEEE 754 bits for a REAL; the bytes in lowercase hexadecimal for TEXT and BLOB; JSON `null` for NULL. |
| `decode_only` | `true` on a valid vector whose `wire` is not the canonical encoding, such as the REAL `1.0` or an escape the encoder does not write. |
| `error` | On a vector that must be refused: the error code, `invalid_request`. A refused vector has no `class` and no `value`. |
| `repeat` | A count. The vector's `wire` is then a JSON string, and the vector stands for the string whose contents are the contents of `wire` repeated that many times. Its `value` is repeated the same number of times. It keeps the vector of the value at the frame size short: 1,048,571 times `a`, whose row frame `["a…a"]` and its newline take exactly 1 MiB. |

A codec passes when it does all three of these:

- It decodes every valid `wire` to `class` and `value`. Decoding compares values, so any valid JSON escaping must decode.
- It encodes the `value` of every valid vector without `decode_only` back to the identical `wire`. Encoding compares bytes, so the encoder must follow the ADR's escaping rule exactly.
- It refuses every vector that has an `error`.
