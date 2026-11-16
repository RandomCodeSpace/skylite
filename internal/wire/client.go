package wire

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"unicode/utf8"
)

// This file is the client's half of the codec: the request bodies a driver
// sends and the frames it reads.

// Path returns the path of a request of kind k on database db, the inverse
// of Route. It returns "" for an invalid name or an unknown kind.
func Path(k Kind, db string) string {
	if !ValidDatabaseName(db) {
		return ""
	}
	base := "/v1/databases/" + db
	switch k {
	case KindQuery:
		return base + "/query"
	case KindDescribe:
		return base + "/describe"
	case KindOpen:
		return base + "/channels"
	case KindChannel:
		return base + "/channel"
	case KindCancel:
		return base + "/channel/cancel"
	case KindClose:
		return base + "/channel/close"
	case KindResolve:
		return base + "/resolve"
	case KindStatus, KindPut:
		return base
	}
	return ""
}

// Call is the body of a request a driver sends.
type Call struct {
	SQL       string
	Params    []any      // positional values: element i binds parameter i+1
	Named     []NamedArg // named values; not with Params
	MinTxn    int64      // 0 is absent
	TimeoutMS int64      // 0 is absent
}

// NamedArg is a named parameter value. Name is the parameter's name without
// its prefix: it binds :Name, @Name and $Name.
type NamedArg struct {
	Name  string
	Value any
}

// AppendCall appends the body of a request of kind k, a query or a
// describe, which Parse accepts. It refuses a Call with fields k does not
// allow, SQL that is empty, holds U+0000 or is not valid UTF-8, an empty,
// invalid or repeated parameter name, and a value AppendValue refuses. Its
// errors never hold a value.
func AppendCall(dst []byte, k Kind, c *Call) ([]byte, error) {
	switch {
	case k != KindQuery && k != KindDescribe:
		return dst, fmt.Errorf("wire: AppendCall does not write request kind %d", k)
	case c.SQL == "" || strings.IndexByte(c.SQL, 0) >= 0 || !utf8.ValidString(c.SQL):
		return dst, errors.New("wire: sql must be non-empty valid UTF-8 without U+0000")
	case len(c.Params) > 0 && len(c.Named) > 0:
		return dst, errors.New("wire: a request uses params or named, not both")
	case k == KindDescribe && (len(c.Params) > 0 || len(c.Named) > 0 || c.TimeoutMS != 0):
		return dst, errors.New("wire: a describe carries sql and min_txn only")
	case c.MinTxn < 0:
		return dst, errors.New("wire: min_txn is negative")
	case c.TimeoutMS < 0 || c.TimeoutMS > math.MaxInt32:
		return dst, errors.New("wire: timeout_ms is outside 1 to 2^31-1")
	}
	o := object{b: append(dst, '{')}
	o.str("sql", c.SQL)
	var err error
	if len(c.Params) > 0 {
		o.key("params")
		o.b = append(o.b, '[')
		for i, v := range c.Params {
			if i > 0 {
				o.b = append(o.b, ',')
			}
			if o.b, err = AppendValue(o.b, v); err != nil {
				return dst, fmt.Errorf("wire: parameter %d: %w", i+1, err)
			}
		}
		o.b = append(o.b, ']')
	}
	if len(c.Named) > 0 {
		o.key("named")
		o.b = append(o.b, '{')
		seen := make(map[string]bool, len(c.Named))
		for i, a := range c.Named {
			if a.Name == "" || !utf8.ValidString(a.Name) || seen[a.Name] {
				return dst, fmt.Errorf("wire: the name of parameter %d is empty, not valid UTF-8 or repeated", i+1)
			}
			seen[a.Name] = true
			if i > 0 {
				o.b = append(o.b, ',')
			}
			o.b = append(appendQuoted(o.b, a.Name), ':')
			if o.b, err = AppendValue(o.b, a.Value); err != nil {
				return dst, fmt.Errorf("wire: parameter %d: %w", i+1, err)
			}
		}
		o.b = append(o.b, '}')
	}
	o.optInt("min_txn", c.MinTxn)
	o.optInt("timeout_ms", c.TimeoutMS)
	return append(o.b, '}'), nil
}

// FrameKind is the kind of a frame a Reader returns.
type FrameKind uint8

// The frame kinds. Keepalives and unknown frames are skipped by Next.
const (
	FrameColumns FrameKind = iota + 1
	FrameRow
	FrameResults
	FrameParams
	FrameDone  // terminal
	FrameError // terminal
)

// Frame is one frame of a response. Only the member of its Kind is set.
// Columns, Row, Results and Params are valid until the next call to Next.
type Frame struct {
	Kind    FrameKind
	Columns []Column
	Row     []any
	Results []Result // this frame's entries
	Params  []string // this frame's names; "" for a parameter without one
	Done    *Done
	Error   *Error
}

// ErrMalformed is wrapped by every error a Reader returns for a body that
// breaks the protocol. A body that ends before its terminal frame is
// io.ErrUnexpectedEOF instead.
var ErrMalformed = errors.New("wire: malformed response")

func malformed(format string, args ...any) error {
	return fmt.Errorf("%w: "+format, append([]any{ErrMalformed}, args...)...)
}

// Done members, as bits of a presence mask.
const (
	dTxn = 1 << iota
	dReadOnly
	dRowsAffected
	dLastInsertRowID
	dParamCount
)

// doneRequired lists, for each done kind a Reader reads, the members it must
// carry. A later kind adds its row here and its members to Reader.done.
var doneRequired = map[DoneKind]int{
	DoneQuery:    dTxn | dReadOnly | dRowsAffected | dLastInsertRowID,
	DoneDescribe: dTxn | dParamCount,
}

// Reader reads a response body one frame at a time, as it arrives, and keeps
// at most one frame in memory. It checks the order of ADR 0005's grammar,
// the row widths, the continuity of results and params entries, the members
// of the terminal frame the request's kind expects, and that nothing follows
// the terminal frame. It skips keepalives, frames whose only member is
// unknown, and unknown members of known objects.
type Reader struct {
	br       *bufio.Reader
	kind     DoneKind
	maxFrame int
	line     []byte
	p        parser
	state    int // the Writer's states
	columns  int
	results  int64 // entries read
	params   int64 // names read
	f        Frame
}

// NewReader returns a Reader of a response to a request whose done frame is
// of kind k, reading frames of at most maxFrame bytes, newline included.
func NewReader(r io.Reader, k DoneKind, maxFrame int) (*Reader, error) {
	if _, ok := doneRequired[k]; !ok {
		return nil, fmt.Errorf("wire: the reader does not read done kind %d", k)
	}
	if maxFrame < 2 {
		return nil, fmt.Errorf("wire: frame size %d is below 2", maxFrame)
	}
	return &Reader{br: bufio.NewReaderSize(r, 64<<10), kind: k, maxFrame: maxFrame}, nil
}

// Next returns the next frame. After the terminal frame it returns io.EOF.
func (r *Reader) Next() (*Frame, error) {
	for {
		if r.state == stateEnded {
			return nil, io.EOF
		}
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if err := checkSyntax(line); err != nil {
			return nil, malformed("a frame is not one JSON value: %v", err)
		}
		r.p = parser{d: newDecoder(line), scratch: r.p.scratch}
		r.f = Frame{}
		skip, err := r.frame(line[0])
		if err != nil {
			return nil, err
		}
		if skip {
			continue
		}
		if r.f.Kind == FrameDone || r.f.Kind == FrameError {
			r.state = stateEnded
			if _, err := r.br.Peek(1); !errors.Is(err, io.EOF) {
				if err == nil {
					return nil, malformed("bytes follow the terminal frame")
				}
				return nil, err
			}
		}
		return &r.f, nil
	}
}

// readLine reads one frame and drops its newline.
func (r *Reader) readLine() ([]byte, error) {
	r.line = r.line[:0]
	for {
		chunk, err := r.br.ReadSlice('\n')
		if len(r.line)+len(chunk) > r.maxFrame {
			return nil, malformed("a frame exceeds %d bytes", r.maxFrame)
		}
		r.line = append(r.line, chunk...)
		switch {
		case err == nil:
			if len(r.line) == 1 {
				return nil, malformed("an empty frame")
			}
			return r.line[:len(r.line)-1], nil
		case errors.Is(err, bufio.ErrBufferFull):
		case errors.Is(err, io.EOF):
			return nil, io.ErrUnexpectedEOF
		default:
			return nil, err
		}
	}
}

// frame decodes the frame in r.p, whose first byte is first, into r.f.
func (r *Reader) frame(first byte) (skip bool, err error) {
	d := r.p.d
	if first == '[' {
		if r.state != stateRows {
			return false, malformed("a row outside the rows of a result")
		}
		return false, r.row()
	}
	if first != '{' {
		return false, malformed("a frame must be an object or a row")
	}
	if _, err := d.ReadToken(); err != nil {
		return false, err
	}
	if d.PeekKind() == '}' {
		return false, malformed("a frame object has no member")
	}
	name, err := r.p.name()
	if err != nil {
		return false, err
	}
	switch string(name) {
	case "keepalive":
		skip, err = true, d.SkipValue()
	case "columns":
		if r.state != stateStart {
			return false, malformed("a columns frame after other frames")
		}
		r.f.Kind, err = FrameColumns, r.columnsFrame()
		r.state, r.columns = stateRows, len(r.f.Columns)
	case "results":
		if r.kind != DoneBatch || r.state == stateParams {
			return false, malformed("a results frame in this response")
		}
		r.f.Kind, err = FrameResults, r.entries(&r.results, "entries", r.result)
		r.state = stateResults
	case "params":
		if r.kind != DoneDescribe && r.kind != DoneChannelDescribe || r.state != stateRows && r.state != stateParams {
			return false, malformed("a params frame in this response or before its columns")
		}
		r.f.Kind, err = FrameParams, r.entries(&r.params, "names", r.param)
		r.state = stateParams
	case "done":
		r.f.Kind, err = FrameDone, r.done()
	case "error":
		r.f.Kind, err = FrameError, r.errorObject()
	default:
		skip, err = true, d.SkipValue()
	}
	if err != nil {
		return false, err
	}
	if d.PeekKind() != '}' {
		return false, malformed("a frame object has more than one member")
	}
	return skip, nil
}

func (r *Reader) row() error {
	d := r.p.d
	if _, err := d.ReadToken(); err != nil {
		return err
	}
	vals := make([]any, 0, r.columns)
	for d.PeekKind() != ']' {
		v, err := readValue(d, &r.p.scratch, true)
		if err != nil {
			return malformed("a row value: %v", err)
		}
		vals = append(vals, v)
	}
	if len(vals) != r.columns {
		return malformed("a row has %d values for %d columns", len(vals), r.columns)
	}
	r.f.Kind, r.f.Row = FrameRow, vals
	return nil
}

// members reads an object, calling member for each member's name; member
// reads the value, and returns false if it is of the wrong type or range. A
// member it does not know it skips, returning true.
func (r *Reader) members(what string, member func(name string) (bool, error)) error {
	d := r.p.d
	if d.PeekKind() != '{' {
		return malformed("%s is not an object", what)
	}
	if _, err := d.ReadToken(); err != nil {
		return err
	}
	for d.PeekKind() != '}' {
		name, err := r.p.name()
		if err != nil {
			return err
		}
		n := string(name)
		ok, err := member(n)
		if err != nil {
			return err
		}
		if !ok {
			return malformed("member %q of %s has the wrong type or range", n, what)
		}
	}
	_, err := d.ReadToken()
	return err
}

func (r *Reader) skip() (bool, error) { return true, r.p.d.SkipValue() }

// boolean reads a JSON boolean.
func (r *Reader) boolean() (v, ok bool, err error) {
	switch r.p.d.PeekKind() {
	case 't', 'f':
		tok, err := r.p.d.ReadToken()
		return err == nil && tok.Bool(), err == nil, err
	}
	return false, false, r.p.d.SkipValue()
}

// nullStr reads a string or null, null as "".
func (r *Reader) nullStr() (string, bool, error) {
	if r.p.d.PeekKind() == 'n' {
		_, err := r.p.d.ReadToken()
		return "", err == nil, err
	}
	return r.p.str()
}

func (r *Reader) columnsFrame() error {
	d := r.p.d
	if d.PeekKind() != '[' {
		return malformed("columns is not an array")
	}
	if _, err := d.ReadToken(); err != nil {
		return err
	}
	var cols []Column
	for d.PeekKind() != ']' {
		var c Column
		hasName := false
		err := r.members("a column", func(n string) (ok bool, err error) {
			switch n {
			case "name":
				c.Name, ok, err = r.p.str()
				hasName = true
			case "decltype":
				c.DeclType, ok, err = r.nullStr()
			case "database":
				c.Database, ok, err = r.nullStr()
			case "table":
				c.Table, ok, err = r.nullStr()
			case "origin":
				c.Origin, ok, err = r.nullStr()
			default:
				return r.skip()
			}
			return ok, err
		})
		if err != nil {
			return err
		}
		if !hasName {
			return malformed("a column without a name")
		}
		cols = append(cols, c)
	}
	_, err := d.ReadToken()
	r.f.Columns = cols
	return err
}

// entries reads {"from":n,<key>:[...]}, the entries of a results or params
// frame, each read by entry; next counts the entries read so far.
func (r *Reader) entries(next *int64, key string, entry func() error) error {
	from, n := int64(-1), int64(-1)
	err := r.members("an entries frame", func(name string) (ok bool, err error) {
		switch name {
		case "from":
			from, ok, err = r.p.integer(0, math.MaxInt64)
			return ok, err
		case key:
			d := r.p.d
			if d.PeekKind() != '[' {
				return false, d.SkipValue()
			}
			if _, err := d.ReadToken(); err != nil {
				return false, err
			}
			n = 0
			for d.PeekKind() != ']' {
				if err := entry(); err != nil {
					return false, err
				}
				n++
			}
			_, err = d.ReadToken()
			return true, err
		}
		return r.skip()
	})
	switch {
	case err != nil:
		return err
	case from != *next:
		return malformed("entries from %d where %d was next", from, *next)
	case n < 1 || n > maxFrameEntries:
		return malformed("an entries frame holds 1 to %d entries", maxFrameEntries)
	}
	*next += n
	return nil
}

func (r *Reader) result() error {
	var e Result
	seen := 0
	err := r.members("a results entry", func(n string) (ok bool, err error) {
		switch n {
		case "rows_affected":
			e.RowsAffected, ok, err = r.p.integer(0, math.MaxInt64)
			seen |= 1
		case "last_insert_rowid":
			e.LastInsertRowID, ok, err = r.p.integer(math.MinInt64, math.MaxInt64)
			seen |= 2
		default:
			return r.skip()
		}
		return ok, err
	})
	if err == nil && seen != 3 {
		err = malformed("a results entry lacks rows_affected or last_insert_rowid")
	}
	r.f.Results = append(r.f.Results, e)
	return err
}

func (r *Reader) param() error {
	s, ok, err := r.nullStr()
	if err == nil && !ok {
		err = malformed("a parameter name is not a string or null")
	}
	r.f.Params = append(r.f.Params, s)
	return err
}

func (r *Reader) done() error {
	d := &Done{Kind: r.kind}
	seen := 0
	err := r.members("done", func(n string) (ok bool, err error) {
		switch n {
		case "txn":
			d.Txn, ok, err = r.p.integer(1, math.MaxInt64)
			seen |= dTxn
		case "read_only":
			d.ReadOnly, ok, err = r.boolean()
			seen |= dReadOnly
		case "rows_affected":
			d.RowsAffected, ok, err = r.p.integer(0, math.MaxInt64)
			seen |= dRowsAffected
		case "last_insert_rowid":
			d.LastInsertRowID, ok, err = r.p.integer(math.MinInt64, math.MaxInt64)
			seen |= dLastInsertRowID
		case "param_count":
			d.ParamCount, ok, err = r.p.integer(0, math.MaxInt32)
			seen |= dParamCount
		default:
			return r.skip()
		}
		return ok, err
	})
	switch need := doneRequired[r.kind]; {
	case err != nil:
		return err
	case seen&need != need:
		return malformed("the done frame lacks a member its request's answer carries")
	case (r.kind == DoneDescribe || r.kind == DoneChannelDescribe) && r.params != d.ParamCount:
		return malformed("%d parameter names for param_count %d", r.params, d.ParamCount)
	case r.kind == DoneDescribe && r.state == stateStart:
		return malformed("a describe answer without its columns frame")
	}
	r.f.Done = d
	return nil
}

func (r *Reader) errorObject() error {
	e := &Error{}
	seen := 0
	err := r.members("error", func(n string) (ok bool, err error) {
		var s string
		switch n {
		case "code":
			s, ok, err = r.p.str()
			e.Code = Code(s)
			seen |= 1
		case "not_executed":
			e.NotExecuted, ok, err = r.boolean()
			seen |= 2
		case "final":
			e.Final, ok, err = r.boolean()
			seen |= 4
		case "message":
			e.Message, ok, err = r.p.str()
			seen |= 8
		case "request_id":
			e.RequestID, ok, err = r.p.str()
			seen |= 16
		case "channel_id":
			e.ChannelID, ok, err = r.p.str()
		case "seq":
			e.Seq, ok, err = r.p.integer(1, math.MaxInt64)
		case "txn_state":
			s, ok, err = r.p.str()
			e.TxnState = TxnState(s)
		case "sqlite_code":
			e.SQLiteCode, ok, err = r.p.integer(0, 255)
		case "sqlite_extended_code":
			e.SQLiteExtendedCode, ok, err = r.p.integer(0, math.MaxInt32)
		case "batch_index":
			var i int64
			i, ok, err = r.p.integer(0, maxBatchEntries-1)
			e.BatchIndex = &i
		case "cause":
			e.Cause, ok, err = r.p.str()
		case "txn":
			e.Txn, ok, err = r.p.integer(1, math.MaxInt64)
		case "epoch":
			e.Epoch, ok, err = r.p.integer(1, math.MaxInt64)
		default:
			return r.skip()
		}
		return ok, err
	})
	if err == nil && seen != 31 {
		err = malformed("the error lacks code, not_executed, final, message or request_id")
	}
	r.f.Error = e
	return err
}
