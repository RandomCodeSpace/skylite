package wire

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/go-json-experiment/json/jsontext"
)

// A value is one of nil (NULL), int64 (INTEGER), float64 (REAL), string
// (TEXT, which may hold invalid UTF-8) and []byte (BLOB).

var strictBase64 = base64.StdEncoding.Strict()

// badValue is a value or field that is well-formed JSON but not allowed by
// the protocol. Every other decoding error is a syntax error.
type badValue string

func (e badValue) Error() string { return string(e) }

// DecodeValue decodes data, which must be exactly one value with nothing
// before or after it.
func DecodeValue(data []byte) (any, error) {
	err := checkSyntax(data)
	var v any
	if err == nil {
		var scratch []byte
		v, err = readValue(newDecoder(data), &scratch, true)
	}
	if err != nil {
		return nil, NewError(CodeInvalidRequest, err.Error())
	}
	return v, nil
}

func isSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// readValue reads one value from d. It always consumes the whole JSON value
// unless it returns a syntax error. Without decode it only checks the value
// and returns nil.
func readValue(d *jsontext.Decoder, scratch *[]byte, decode bool) (any, error) {
	switch d.PeekKind() {
	case 'n':
		_, err := d.ReadToken()
		return nil, err
	case '0':
		raw, err := d.ReadValue()
		if err != nil {
			return nil, err
		}
		if bytes.ContainsAny(raw, ".eE") {
			f, err := strconv.ParseFloat(string(raw), 64)
			if err != nil { // the syntax is valid, so only overflow is left
				return nil, badValue("a REAL must be a finite double")
			}
			return f, nil
		}
		n, ok := parseInt(raw)
		if !ok {
			return nil, badValue("an INTEGER must be in the signed 64-bit range")
		}
		return n, nil
	case '"':
		raw, err := d.ReadValue()
		if err != nil || !decode {
			return nil, err
		}
		s, err := unquote(raw, scratch)
		return string(s), err
	case '{':
		return readTagged(d, scratch, decode)
	case 't', 'f', '[':
		if err := d.SkipValue(); err != nil {
			return nil, err
		}
		return nil, badValue("a value must be null, a number, a string or a tagged object")
	}
	_, err := d.ReadValue() // reports the syntax error
	if err == nil {
		err = errors.New("wire: unexpected JSON kind")
	}
	return nil, err
}

// readTagged reads {"real":...}, {"text_bytes":...} or {"blob":...}.
func readTagged(d *jsontext.Decoder, scratch *[]byte, decode bool) (any, error) {
	if _, err := d.ReadToken(); err != nil {
		return nil, err
	}
	if d.PeekKind() == '}' {
		_, err := d.ReadToken()
		return nil, errOr(err, "a tagged value needs one member")
	}
	raw, err := d.ReadValue()
	if err != nil {
		return nil, err
	}
	name, err := unquote(raw, scratch)
	if err != nil {
		return nil, err
	}
	tag := string(name)
	if d.PeekKind() != '"' {
		if err := skipRest(d); err != nil {
			return nil, err
		}
		return nil, badValue("a tagged value's member must be a string")
	}
	if raw, err = d.ReadValue(); err != nil {
		return nil, err
	}
	s, err := unquote(raw, scratch)
	if err != nil {
		return nil, err
	}
	var v any
	var bad badValue
	switch tag {
	case "real":
		switch string(s) {
		case "Infinity":
			v = math.Inf(1)
		case "-Infinity":
			v = math.Inf(-1)
		default:
			bad = `"real" must be "Infinity" or "-Infinity"`
		}
	case "text_bytes", "blob":
		b, ok := decodeBase64(s)
		switch {
		case !ok:
			bad = "invalid base64"
		case !decode:
		case tag == "blob":
			v = b
		default:
			v = string(b)
		}
	default:
		bad = "unknown value tag"
	}
	if d.PeekKind() != '}' {
		if err := skipRest(d); err != nil {
			return nil, err
		}
		return nil, badValue("a tagged value has exactly one member")
	}
	if _, err := d.ReadToken(); err != nil {
		return nil, err
	}
	if bad != "" {
		return nil, bad
	}
	return v, nil
}

// skipRest consumes the remaining members of an object and its closing brace.
func skipRest(d *jsontext.Decoder) error {
	for d.PeekKind() != '}' {
		if err := d.SkipValue(); err != nil {
			return err
		}
	}
	_, err := d.ReadToken()
	return err
}

func errOr(err error, msg badValue) error {
	if err != nil {
		return err
	}
	return msg
}

// decodeBase64 decodes standard base64 with padding, no line breaks and zero
// unused bits. The standard library's decoder skips CR and LF, so they are
// refused first.
func decodeBase64(s []byte) ([]byte, bool) {
	if bytes.ContainsAny(s, "\r\n") {
		return nil, false
	}
	b := make([]byte, strictBase64.DecodedLen(len(s)))
	n, err := strictBase64.Decode(b, s)
	return b[:n], err == nil
}

// unquote returns the contents of a valid JSON string. Without escapes it
// returns a slice of raw; otherwise it unescapes into *scratch.
func unquote(raw []byte, scratch *[]byte) ([]byte, error) {
	if bytes.IndexByte(raw, '\\') < 0 {
		return raw[1 : len(raw)-1], nil
	}
	b, err := jsontext.AppendUnquote((*scratch)[:0], raw)
	*scratch = b
	return b, err
}

// parseInt parses a JSON number without fraction or exponent exactly.
func parseInt(raw []byte) (int64, bool) {
	neg := raw[0] == '-'
	if neg {
		raw = raw[1:]
	}
	limit := uint64(math.MaxInt64)
	if neg {
		limit++
	}
	var n uint64
	for _, c := range raw {
		d := uint64(c - '0')
		if n > (limit-d)/10 {
			return 0, false
		}
		n = n*10 + d
	}
	if neg {
		return int64(-n), true //nolint:gosec // -2^63 wraps to itself, as intended
	}
	return int64(n), true //nolint:gosec // n <= MaxInt64
}

// AppendValue appends the canonical encoding of v, one of the value types.
func AppendValue(dst []byte, v any) ([]byte, error) {
	switch v := v.(type) {
	case nil:
		return append(dst, "null"...), nil
	case int64:
		return strconv.AppendInt(dst, v, 10), nil
	case float64:
		switch {
		case math.IsNaN(v):
			return dst, errors.New("wire: NaN has no encoding")
		case math.IsInf(v, 1):
			return append(dst, `{"real":"Infinity"}`...), nil
		case math.IsInf(v, -1):
			return append(dst, `{"real":"-Infinity"}`...), nil
		}
		return appendReal(dst, v), nil
	case string:
		if utf8.ValidString(v) {
			return jsontext.AppendQuote(dst, v)
		}
		return appendBase64(append(dst, `{"text_bytes":"`...), []byte(v)), nil
	case []byte:
		return appendBase64(append(dst, `{"blob":"`...), v), nil
	}
	return dst, fmt.Errorf("wire: %T is not a value type", v)
}

func appendBase64(dst, b []byte) []byte {
	return append(base64.StdEncoding.AppendEncode(dst, b), `"}`...)
}

// FormatReal returns the canonical form of a finite double: the shortest
// digits that parse back to f, as [-]d[.ddd]e[-]N.
func FormatReal(f float64) string { return string(appendReal(nil, f)) }

func appendReal(dst []byte, f float64) []byte {
	var tmp [32]byte
	b := strconv.AppendFloat(tmp[:0], f, 'e', -1, 64) // d.ddde±dd
	i := bytes.IndexByte(b, 'e')
	dst = append(dst, b[:i+1]...)
	if b[i+1] == '-' {
		dst = append(dst, '-')
	}
	exp := bytes.TrimLeft(b[i+2:], "0")
	if len(exp) == 0 {
		return append(dst, '0')
	}
	return append(dst, exp...)
}
