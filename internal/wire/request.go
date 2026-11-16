package wire

import (
	"bytes"
	"fmt"
	"math"
	"math/bits"
	"strings"

	"github.com/go-json-experiment/json/jsontext"
)

// Kind is what a request's method and path ask for.
type Kind uint8

// The request kinds, one per row of ADR 0005's path table.
const (
	KindQuery    Kind = iota + 1 // POST /v1/databases/{db}/query
	KindDescribe                 // POST /v1/databases/{db}/describe
	KindOpen                     // POST /v1/databases/{db}/channels
	KindChannel                  // POST /v1/databases/{db}/channel
	KindCancel                   // POST /v1/databases/{db}/channel/cancel
	KindClose                    // POST /v1/databases/{db}/channel/close
	KindResolve                  // POST /v1/databases/{db}/resolve
	KindStatus                   // GET /v1/databases/{db}
	KindPut                      // PUT /v1/databases/{db}
)

// ControlBodyBytes is the body limit of cancel, close and resolve, fixed by
// the protocol.
const ControlBodyBytes = 4096

// ValidDatabaseName reports whether s is 1 to 63 characters from a-z, 0-9
// and -, starting with a letter or digit.
func ValidDatabaseName(s string) bool {
	if len(s) == 0 || len(s) > 63 || s[0] == '-' {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return false
		}
	}
	return true
}

// Route returns the kind and database name of a request. path is the
// request's path as sent, without its query.
func Route(method, path string) (Kind, string, *Error) {
	rest, ok := strings.CutPrefix(path, "/v1/databases/")
	db, sub, hasSub := strings.Cut(rest, "/")
	if ok && ValidDatabaseName(db) {
		switch {
		case !hasSub && method == "GET":
			return KindStatus, db, nil
		case !hasSub && method == "PUT":
			return KindPut, db, nil
		case hasSub && method == "POST":
			switch sub {
			case "query":
				return KindQuery, db, nil
			case "describe":
				return KindDescribe, db, nil
			case "channels":
				return KindOpen, db, nil
			case "channel":
				return KindChannel, db, nil
			case "channel/cancel":
				return KindCancel, db, nil
			case "channel/close":
				return KindClose, db, nil
			case "resolve":
				return KindResolve, db, nil
			}
		}
	}
	return 0, "", NewError(CodeInvalidRequest, "unknown method or path")
}

// Op is a sequenced request's operation.
type Op uint8

// The operations. OpUnknown is an op the protocol does not define.
const (
	OpUnknown Op = iota
	OpBegin
	OpExecute
	OpBatch
	OpDescribe
	OpCommit
	OpRollback
	OpPing
)

var opNames = map[string]Op{
	"begin": OpBegin, "execute": OpExecute, "batch": OpBatch, "describe": OpDescribe,
	"commit": OpCommit, "rollback": OpRollback, "ping": OpPing,
}

// Mode is a channel's kind.
type Mode uint8

// The channel modes.
const (
	ModeWrite Mode = iota + 1
	ModeReadOnly
)

func (m Mode) String() string {
	switch m {
	case ModeWrite:
		return "write"
	case ModeReadOnly:
		return "read_only"
	}
	return ""
}

// Args are a statement's parameter values, kept as the position of their
// JSON array or object in the request body, so that a body of many small
// values decodes into little memory. The zero Args has no parameters.
type Args struct {
	off   uint32
	named bool
}

// Statement is one SQL statement and its arguments.
type Statement struct {
	SQL  string
	Args Args
}

// Request is a parsed request body. Absent integers are zero; an absent
// timeout_ms is zero. It refers to the body it was parsed from, which must
// not change while the Request is used.
type Request struct {
	Kind       Kind
	Channel    string
	Seq        int64
	Op         Op
	Mode       Mode
	MinTxn     int64
	TimeoutMS  int64
	Statement             // sql with params or named, of query, describe, execute and a batch with param_sets
	Execute    *Statement // a begin's first statement
	ParamSets  []Args     // a batch of one statement
	Statements []Statement

	body    []byte
	invalid *Error
}

// Field bits.
const (
	fSQL = 1 << iota
	fParams
	fNamed
	fMinTxn
	fTimeout
	fMode
	fChannel
	fSeq
	fOp
	fExecute
	fParamSets
	fStatements
)

// fieldNames lists the top-level fields in bit order.
var fieldNames = []string{
	"sql", "params", "named", "min_txn", "timeout_ms", "mode", "channel", "seq", "op", "execute",
	"param_sets", "statements",
}

func fieldBit(name []byte) int {
	for i, n := range fieldNames {
		if string(name) == n {
			return 1 << i
		}
	}
	return 0
}

// Parse frames and identifies a request body before acceptance, and checks
// everything that needs no SQLite. batchLimit is the most entries a batch
// may have.
//
// A refusal is an invalid_request error with the flags of an answer before
// acceptance. For a channel request, Parse refuses only a body that is not
// one well-formed JSON object or does not identify its channel and sequence
// number; every other problem is left for Validate, to be recorded. Every
// other kind is checked completely here.
func Parse(kind Kind, body []byte, batchLimit int) (*Request, *Error) {
	if len(body) == 0 || body[0] != '{' || len(body) > math.MaxUint32 {
		return nil, NewError(CodeInvalidRequest, "the body must be one JSON object, starting at its first byte")
	}
	if err := checkSyntax(body); err != nil {
		return nil, NewError(CodeInvalidRequest, "the body is not one JSON object: "+err.Error())
	}
	p := parser{d: newDecoder(body), batchLimit: batchLimit}
	p.r.Kind, p.r.body = kind, body
	if err := p.object(); err != nil {
		return nil, NewError(CodeInvalidRequest, "the body is not one JSON object: "+err.Error())
	}
	p.shape()
	r := &p.r
	needSeq := kind == KindChannel || kind == KindCancel || kind == KindResolve
	if (needSeq || kind == KindClose) && !p.has(fChannel) {
		return nil, NewError(CodeInvalidRequest, "the request must carry channel, a string of the token's form")
	}
	if needSeq && !p.has(fSeq) {
		return nil, NewError(CodeInvalidRequest, "the request must carry seq, an integer from 1 to 2^63-1")
	}
	if kind == KindChannel {
		r.invalid = p.bad
		return r, nil
	}
	if p.bad != nil {
		return nil, p.bad
	}
	return r, nil
}

// Validate returns the first problem Parse found in a channel request after
// identifying it, as an invalid_request error with the flags of an answer
// before acceptance; the owner records it with Recorded. It returns nil for
// a valid request.
func (r *Request) Validate() *Error {
	if r.invalid == nil {
		return nil
	}
	e := *r.invalid
	return &e
}

type parser struct {
	d             *jsontext.Decoder
	r             Request
	batchLimit    int
	scratch       []byte
	seen, badBits int // field bits present, and present but bad
	bad           *Error
}

func (p *parser) has(f int) bool { return p.seen&f != 0 && p.badBits&f == 0 }

// fail records the first problem found.
func (p *parser) fail(format string, args ...any) {
	if p.bad == nil {
		p.bad = NewError(CodeInvalidRequest, fmt.Sprintf(format, args...))
	}
}

func (p *parser) object() error {
	if _, err := p.d.ReadToken(); err != nil {
		return err
	}
	for p.d.PeekKind() != '}' {
		name, err := p.name()
		if err != nil {
			return err
		}
		f := fieldBit(name)
		p.seen |= f
		if err := p.field(f, name); err != nil {
			return err
		}
	}
	_, err := p.d.ReadToken()
	return err
}

// name reads an object member's name, valid until the next read.
func (p *parser) name() ([]byte, error) {
	raw, err := p.d.ReadValue()
	if err != nil {
		return nil, err
	}
	return unquote(raw, &p.scratch)
}

// field reads the value of top-level field f, whose name is valid only until
// the value is read. It formats a message for an unknown field only if it is
// the first problem, so that many unknown fields allocate nothing each.
func (p *parser) field(f int, name []byte) error {
	r := &p.r
	var ok bool
	var err error
	switch f {
	case fChannel:
		r.Channel, ok, err = p.str()
		ok = ok && validToken(r.Channel)
	case fSeq:
		r.Seq, ok, err = p.integer(1, math.MaxInt64)
	case fOp:
		var s string
		s, ok, err = p.str()
		r.Op = opNames[s]
		ok = ok && r.Op != OpUnknown
	case fMode:
		var s string
		s, ok, err = p.str()
		switch s {
		case "write":
			r.Mode = ModeWrite
		case "read_only":
			r.Mode = ModeReadOnly
		default:
			ok = false
		}
	case fMinTxn:
		r.MinTxn, ok, err = p.integer(0, math.MaxInt64)
	case fTimeout:
		r.TimeoutMS, ok, err = p.integer(1, math.MaxInt32)
	case fSQL:
		r.SQL, ok, err = p.sql()
	case fParams:
		r.Args, ok, err = p.args('[')
	case fNamed:
		r.Args, ok, err = p.args('{')
	case fExecute:
		var s Statement
		s, ok, err = p.statement()
		r.Execute = &s
	case fParamSets:
		var first jsontext.Kind
		r.ParamSets = make([]Args, 0, p.capacity(len("[],")))
		ok, err = p.list(func() (bool, error) {
			k := p.d.PeekKind()
			if k != '{' {
				k = '[' // anything else is refused by args
			}
			if first == 0 {
				first = k
			}
			a, ok, err := p.args(k)
			ok = ok && k == first
			if ok && len(r.ParamSets) < cap(r.ParamSets) {
				r.ParamSets = append(r.ParamSets, a)
			}
			return ok, err
		})
	case fStatements:
		r.Statements = make([]Statement, 0, p.capacity(len(`{"sql":""},`)))
		ok, err = p.list(func() (bool, error) {
			s, ok, err := p.statement()
			if ok && len(r.Statements) < cap(r.Statements) {
				r.Statements = append(r.Statements, s)
			}
			return ok, err
		})
	default:
		if p.bad == nil {
			p.fail("unknown field %q", name)
		}
		return p.d.SkipValue()
	}
	if err != nil {
		return err
	}
	if !ok {
		p.badBits |= f
		p.fail("field %q has a bad value", fieldNames[bits.TrailingZeros(uint(f))]) // at most once per known field
	}
	return nil
}

// str reads a string.
func (p *parser) str() (string, bool, error) {
	if p.d.PeekKind() != '"' {
		return "", false, p.d.SkipValue()
	}
	raw, err := p.d.ReadValue()
	if err != nil {
		return "", false, err
	}
	s, err := unquote(raw, &p.scratch)
	return string(s), err == nil, err
}

// integer reads an integer in [lo, hi].
func (p *parser) integer(lo, hi int64) (int64, bool, error) {
	if p.d.PeekKind() != '0' {
		return 0, false, p.d.SkipValue()
	}
	raw, err := p.d.ReadValue()
	if err != nil || bytes.ContainsAny(raw, ".eE") {
		return 0, false, err
	}
	n, ok := parseInt(raw)
	return n, ok && n >= lo && n <= hi, nil
}

// sql reads SQL text, which may not contain U+0000.
func (p *parser) sql() (string, bool, error) {
	s, ok, err := p.str()
	return s, ok && strings.IndexByte(s, 0) < 0, err
}

// args reads a params array (open '[') or a named object (open '{') and
// checks its values, keeping only its position.
func (p *parser) args(open jsontext.Kind) (Args, bool, error) {
	if p.d.PeekKind() != open {
		return Args{}, false, p.d.SkipValue()
	}
	if _, err := p.d.ReadToken(); err != nil {
		return Args{}, false, err
	}
	a := Args{off: uint32(p.d.InputOffset() - 1), named: open == '{'} //nolint:gosec // Parse bounds the body to 4 GiB
	end := jsontext.Kind(']')
	if a.named {
		end = '}'
	}
	ok := true
	for p.d.PeekKind() != end {
		if a.named {
			if _, err := p.d.ReadValue(); err != nil { // the name
				return a, false, err
			}
		}
		if _, err := readValue(p.d, &p.scratch, false); err != nil {
			bad, isBad := err.(badValue) //nolint:errorlint // readValue returns badValue unwrapped
			if !isBad {
				return a, false, err
			}
			ok = false
			if p.bad == nil { // fail's arguments are boxed even when it records nothing
				p.fail("bad parameter value: %s", string(bad))
			}
		}
	}
	_, err := p.d.ReadToken()
	return a, ok, err
}

// statement reads {"sql":..., "params" or "named":...}.
func (p *parser) statement() (Statement, bool, error) {
	var s Statement
	if p.d.PeekKind() != '{' {
		return s, false, p.d.SkipValue()
	}
	if _, err := p.d.ReadToken(); err != nil {
		return s, false, err
	}
	ok, hasSQL, hasArgs := true, false, false
	for p.d.PeekKind() != '}' {
		name, err := p.name()
		if err != nil {
			return s, false, err
		}
		var good bool
		switch string(name) {
		case "sql":
			s.SQL, good, err = p.sql()
			hasSQL = true
		case "params", "named":
			open := jsontext.Kind('[')
			if string(name) == "named" {
				open = '{'
			}
			s.Args, good, err = p.args(open)
			good = good && !hasArgs
			hasArgs = true
		default:
			good, err = false, p.d.SkipValue()
		}
		if err != nil {
			return s, false, err
		}
		ok = ok && good
	}
	_, err := p.d.ReadToken()
	return s, ok && hasSQL, err
}

// capacity bounds the entries of a batch array that remain to be read, each
// valid entry taking at least size bytes, so that the array is allocated
// once and in proportion to the body.
func (p *parser) capacity(size int) int {
	return max(0, min(p.batchLimit, (len(p.r.body)-int(p.d.InputOffset()))/size+1))
}

// list reads an array of 1 to batchLimit elements, each read by elem.
func (p *parser) list(elem func() (bool, error)) (bool, error) {
	if p.d.PeekKind() != '[' {
		return false, p.d.SkipValue()
	}
	if _, err := p.d.ReadToken(); err != nil {
		return false, err
	}
	ok, n := true, 0
	for p.d.PeekKind() != ']' {
		good, err := elem()
		if err != nil {
			return false, err
		}
		ok = ok && good
		n++
	}
	_, err := p.d.ReadToken()
	return ok && n >= 1 && n <= p.batchLimit, err
}

// shape checks the fields a request has against those its kind and op
// require and allow.
func (p *parser) shape() {
	r := &p.r
	var req, opt int
	switch r.Kind {
	case KindQuery:
		req, opt = fSQL, fParams|fNamed|fMinTxn|fTimeout
	case KindDescribe:
		req, opt = fSQL, fMinTxn
	case KindOpen:
		req = fMode
		if r.Mode == ModeReadOnly {
			opt = fMinTxn
		}
	case KindCancel, KindResolve:
		req = fChannel | fSeq
	case KindClose:
		req = fChannel
	case KindPut:
	case KindChannel:
		req = fChannel | fSeq | fOp
		switch r.Op {
		case OpBegin:
			opt = fMinTxn | fExecute | fTimeout
		case OpExecute:
			req, opt = req|fSQL, fParams|fNamed|fMinTxn|fTimeout
		case OpBatch:
			opt = fMinTxn | fTimeout
			if p.seen&fStatements != 0 {
				req |= fStatements
			} else {
				req |= fSQL | fParamSets
			}
		case OpDescribe:
			req |= fSQL
		case OpCommit:
			opt = fTimeout
		case OpRollback, OpPing, OpUnknown:
		}
	default:
		p.fail("unknown request kind")
		return
	}
	for i, name := range fieldNames {
		f := 1 << i
		switch {
		case p.seen&f != 0 && (req|opt)&f == 0:
			p.fail("field %q is not allowed in this request", name)
		case p.seen&f == 0 && req&f != 0:
			p.fail("field %q is required in this request", name)
		}
	}
	if p.seen&fParams != 0 && p.seen&fNamed != 0 {
		p.fail("a request uses params or named, not both")
	}
}

// validToken reports whether s has the token's form: 1 to 256 characters
// from A-Z a-z 0-9 . _ -.
func validToken(s string) bool {
	if len(s) == 0 || len(s) > 256 {
		return false
	}
	for i := range len(s) {
		c := s[i]
		if (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// Bind passes a statement's argument values to set, by parameter index from
// 1, after checking that they bind every parameter exactly once: set is not
// called unless they do. names holds the statement's parameter names by
// index from 1, names[0] being that of parameter 1, with their prefix as
// sqlite3_bind_parameter_name reports them, or "" for a parameter without a
// name. A channel request that Validate finds invalid returns Validate's
// error before anything else. A binding mismatch is an invalid_request
// *Error with the flags of an answer before acceptance; an error from set is
// returned unchanged. Bind decodes one value at a time and keeps none.
func (r *Request) Bind(a Args, names []string, set func(index int, v any) error) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if a.off == 0 {
		if len(names) > 0 {
			return NewError(CodeInvalidRequest, "parameter 1 is not bound")
		}
		return nil
	}
	var index map[string]int
	if a.named {
		index = make(map[string]int, len(names))
		for i, n := range names {
			if n == "" || strings.IndexByte(":@$", n[0]) < 0 {
				continue
			}
			if _, dup := index[n[1:]]; dup {
				return NewError(CodeInvalidRequest, "parameters "+n+" and another differ only in their prefix; bind them with params")
			}
			index[n[1:]] = i + 1
		}
	}
	if err := r.bindValues(a, len(names), index, nil); err != nil {
		return err
	}
	return r.bindValues(a, len(names), index, set)
}

// bindValues reads a's values in order. Without set it checks that they
// bind every one of n parameters exactly once; with set it passes them on.
func (r *Request) bindValues(a Args, n int, index map[string]int, set func(int, any) error) error {
	d := newDecoder(r.body[a.off:])
	if _, err := d.ReadToken(); err != nil {
		return NewError(CodeInvalidRequest, err.Error())
	}
	var scratch []byte
	var bound []bool
	if set == nil {
		bound = make([]bool, n+1)
	}
	for i := 1; d.PeekKind() != ']' && d.PeekKind() != '}'; i++ {
		at := i
		if index != nil {
			raw, err := d.ReadValue()
			if err != nil {
				return NewError(CodeInvalidRequest, err.Error())
			}
			key, err := unquote(raw, &scratch)
			if err != nil {
				return NewError(CodeInvalidRequest, err.Error())
			}
			var ok bool
			if at, ok = index[string(key)]; !ok {
				return NewError(CodeInvalidRequest, fmt.Sprintf("no parameter is named %q", key))
			}
		} else if at > n {
			return NewError(CodeInvalidRequest, fmt.Sprintf("the statement has %d parameters and params has more values", n))
		}
		v, err := readValue(d, &scratch, set != nil)
		if err != nil {
			return NewError(CodeInvalidRequest, err.Error())
		}
		if set == nil {
			bound[at] = true
			continue
		}
		if err := set(at, v); err != nil {
			return err
		}
	}
	for i := 1; i < len(bound); i++ {
		if !bound[i] {
			return NewError(CodeInvalidRequest, fmt.Sprintf("parameter %d is not bound", i))
		}
	}
	return nil
}
