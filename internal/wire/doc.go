// Package wire encodes and decodes the client protocol of ADR 0005
// (docs/content/docs/decisions/0005-client-protocol.md): request paths and
// bodies, SQL values, response frames, done fields, the limits object and
// errors. The HTTPS server and the Go driver share it. It does no I/O
// beyond the writer it is given, and knows nothing of channels, SQLite or
// authentication.
//
// JSON is read with github.com/go-json-experiment/json/jsontext, which
// refuses invalid UTF-8 and lone surrogate escapes. Its duplicate-name check
// is turned off, because its memory per name is too large; this package
// finds duplicate names itself. This package adds the protocol's other
// rules: a body is exactly one object from its first byte to its last,
// nested at most MaxDepth deep, integers are exact and in range, REALs are
// finite, base64 is strict, and parameters bind exactly once. It writes JSON
// strings with its own escaping rule, not jsontext's.
package wire
