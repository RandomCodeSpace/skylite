// Package wire encodes and decodes the client protocol of ADR 0005
// (docs/content/docs/decisions/0005-client-protocol.md): request paths and
// bodies, SQL values, response frames, done fields, the limits object and
// errors. The HTTPS server and the Go driver share it. It does no I/O
// beyond the writer it is given, and knows nothing of channels, SQLite or
// authentication.
//
// JSON is read with github.com/go-json-experiment/json/jsontext, which
// refuses duplicate object names, invalid UTF-8 and lone surrogate escapes.
// This package adds the protocol's other rules: a body is exactly one
// object from its first byte to its last, integers are exact and in range,
// REALs are finite, base64 is strict, and parameters bind exactly once.
package wire
