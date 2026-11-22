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
	Mode      Mode       // an open's
	Channel   string     // the token, of a channel request, a cancel and a close
	Seq       int64      // of a channel request and a cancel
	Op        Op         // of a channel request
}

// NamedArg is a named parameter value. Name is the parameter's name without
// its prefix: it binds :Name, @Name and $Name.
type NamedArg struct {
	Name  string
	Value any
}

// AppendCall appends the body of a request of kind k, which Parse accepts:
// a query, a describe, an open, a channel request with op begin, execute,
// commit, rollback or ping, a cancel or a close. It refuses a Call with
// fields k (and its op) does not allow, SQL that is empty, holds U+0000 or
// is not valid UTF-8, an empty, invalid or repeated parameter name, and a
// value AppendValue refuses. Its errors never hold a value or a token.
func AppendCall(dst []byte, k Kind, c *Call) ([]byte, error) {
	// The fields each kind and op allows: sql (with params and named),
	// min_txn, timeout_ms, mode, channel, seq and op.
	var allow int
	switch k {
	case KindQuery:
		allow = fSQL | fMinTxn | fTimeout
	case KindDescribe:
		allow = fSQL | fMinTxn
	case KindOpen:
		allow = fMode
		if c.Mode == ModeReadOnly {
			allow |= fMinTxn
		}
	case KindChannel:
		allow = fChannel | fSeq | fOp
		switch c.Op {
		case OpBegin:
			allow |= fMinTxn | fTimeout
		case OpExecute:
			allow |= fSQL | fMinTxn | fTimeout
		case OpCommit:
			allow |= fTimeout
		case OpRollback, OpPing:
		default:
			return dst, fmt.Errorf("wire: AppendCall does not write op %d", c.Op)
		}
	case KindCancel:
		allow = fChannel | fSeq
	case KindClose:
		allow = fChannel
	default:
		return dst, fmt.Errorf("wire: AppendCall does not write request kind %d", k)
	}
	if allow&fSQL != 0 && k != KindDescribe {
		allow |= fParams
	}
	has := 0
	for _, m := range []struct {
		f   int
		set bool
	}{
		{fSQL, c.SQL != ""},
		{fParams, len(c.Params) > 0 || len(c.Named) > 0},
		{fMinTxn, c.MinTxn != 0},
		{fTimeout, c.TimeoutMS != 0},
		{fMode, c.Mode != 0},
		{fChannel, c.Channel != ""},
		{fSeq, c.Seq != 0},
		{fOp, c.Op != 0},
	} {
		if m.set {
			has |= m.f
		}
	}
	need := allow &^ (fMinTxn | fTimeout | fParams)
	switch {
	case has&^allow != 0 || has&need != need:
		return dst, fmt.Errorf("wire: a request of kind %d carries a field it does not allow or lacks one it needs", k)
	case strings.IndexByte(c.SQL, 0) >= 0 || !utf8.ValidString(c.SQL):
		return dst, errors.New("wire: sql must be non-empty valid UTF-8 without U+0000")
	case len(c.Params) > 0 && len(c.Named) > 0:
		return dst, errors.New("wire: a request uses params or named, not both")
	case c.MinTxn < 0:
		return dst, errors.New("wire: min_txn is negative")
	case c.TimeoutMS < 0 || c.TimeoutMS > math.MaxInt32:
		return dst, errors.New("wire: timeout_ms is outside 1 to 2^31-1")
	case c.Seq < 0:
		return dst, errors.New("wire: seq is outside 1 to 2^63-1")
	case c.Mode != 0 && c.Mode != ModeWrite && c.Mode != ModeReadOnly:
		return dst, errors.New("wire: mode is neither write nor read_only")
	case c.Channel != "" && !validToken(c.Channel):
		return dst, errors.New("wire: channel is not 1 to 256 characters from A-Z a-z 0-9 . _ -")
	}
	o := object{b: append(dst, '{')}
	if c.Mode != 0 {
		o.str("mode", c.Mode.String())
	}
	if c.Channel != "" {
		o.str("channel", c.Channel)
	}
	o.optInt("seq", c.Seq)
	for name, op := range opNames {
		if op == c.Op {
			o.str("op", name)
		}
	}
	if c.SQL != "" {
		o.str("sql", c.SQL)
	}
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
	FrameParams
	FrameDone  // terminal
	FrameError // terminal
)

// Frame is one frame of a response. Only the member of its Kind is set.
// Columns, Row and Params are valid until the next call to Next.
type Frame struct {
	Kind    FrameKind
	Columns []Column
	Row     []any
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
	dSeq
	dTxnState
	dChannel
	dCommitted
	dWrote
	dLastSeq
)

// doneRequired lists, for each done kind a Reader reads, the members it must
// carry. A later kind adds its row here and its members to Reader.done.
var doneRequired = map[DoneKind]int{
	DoneQuery:    dTxn | dReadOnly | dRowsAffected | dLastInsertRowID,
	DoneDescribe: dTxn | dParamCount,
	DoneOpen:     dTxn | dChannel,
	DoneBegin:    dSeq | dTxnState | dTxn,
	DoneExecute:  dSeq | dTxnState | dTxn | dReadOnly | dRowsAffected | dLastInsertRowID,
	DoneCommit:   dSeq | dTxnState | dTxn | dCommitted | dWrote,
	DoneRollback: dSeq | dTxnState | dTxn,
	DoneClose:    dLastSeq, // its outcome is skipped
}

// terminalFrameBytes bounds a terminal frame, newline included: ADR 0005
// reserves 4 KiB for it, and no configuration makes one larger.
const terminalFrameBytes = 4 << 10

// Reader reads a response body one frame at a time, as it arrives, and keeps
// at most one frame in memory. It checks the order of ADR 0005's grammar,
// the row widths, the continuity of params entries, the members of the
// terminal frame the request's kind expects, the terminal frame's size, and
// that nothing follows the terminal frame. It skips keepalives, frames whose
// only member is unknown, and unknown members of known objects.
type Reader struct {
	br       *bufio.Reader
	kind     DoneKind
	maxFrame int
	line     []byte
	p        parser
	state    int // the Writer's states
	columns  int
	params   int64 // names read
	result   int64 // bytes of the frames before the terminal frame
	limit    int64 // the most result may reach; 0 for no limit
	f        Frame
}

// LimitResult makes Next refuse a response whose frames before the
// terminal frame pass n bytes, newlines included: a channel's advertised
// result_bytes, which its owner never exceeds.
func (r *Reader) LimitResult(n int64) { r.limit = n }

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
		if r.f.Kind != FrameDone && r.f.Kind != FrameError {
			if r.result += int64(len(r.line)); r.limit > 0 && r.result > r.limit {
				return nil, malformed("the result exceeds the channel's result_bytes of %d", r.limit)
			}
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
		if r.state != stateRows || r.kind == DoneDescribe {
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
		if r.kind != DoneQuery && r.kind != DoneExecute && r.kind != DoneDescribe {
			return false, malformed("a columns frame in this response")
		}
		r.f.Kind, err = FrameColumns, r.columnsFrame()
		r.state, r.columns = stateRows, len(r.f.Columns)
	case "results": // only in a batch answer, which no Reader reads yet
		return false, malformed("a results frame in this response")
	case "params":
		if r.kind != DoneDescribe || r.state != stateRows && r.state != stateParams {
			return false, malformed("a params frame in this response or before its columns")
		}
		r.f.Kind, err = FrameParams, r.entries(&r.params, "names", r.param)
		r.state = stateParams
	case "done", "error":
		if len(r.line) > terminalFrameBytes { // r.line still holds the newline
			return false, malformed("a terminal frame exceeds %d bytes", terminalFrameBytes)
		}
		if string(name) == "done" {
			r.f.Kind, err = FrameDone, r.done()
		} else {
			r.f.Kind, err = FrameError, r.errorObject()
		}
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
		if len(vals) == r.columns { // before decoding a value it would drop
			return malformed("a row has more values than its %d columns", r.columns)
		}
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
				if n == maxFrameEntries { // before parsing an entry it would refuse
					return false, malformed("an entries frame holds 1 to %d entries", maxFrameEntries)
				}
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
		case "seq":
			d.Seq, ok, err = r.p.integer(1, math.MaxInt64)
			seen |= dSeq
		case "txn_state":
			var s string
			s, ok, err = r.p.str()
			d.TxnState = TxnState(s)
			ok = ok && (d.TxnState == TxnNone || d.TxnState == TxnOpen || d.TxnState == TxnAborted)
			seen |= dTxnState
		case "channel":
			d.Channel = &Channel{}
			ok, err = true, r.channel(d.Channel)
			seen |= dChannel
		case "committed":
			var c bool
			c, ok, err = r.boolean()
			ok = ok && c
			seen |= dCommitted
		case "wrote":
			d.Wrote, ok, err = r.boolean()
			seen |= dWrote
		case "data_omitted":
			d.DataOmitted, ok, err = r.boolean()
		case "last_seq":
			d.LastSeq, ok, err = r.p.integer(0, math.MaxInt64)
			seen |= dLastSeq
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
	case r.kind == DoneDescribe && r.params != d.ParamCount:
		return malformed("%d parameter names for param_count %d", r.params, d.ParamCount)
	case r.kind == DoneDescribe && r.state == stateStart:
		return malformed("a describe answer without its columns frame")
	}
	r.f.Done = d
	return nil
}

// channel reads an open answer's channel object; it requires the token,
// the identifier and, of the limits, the two a driver enforces:
// request_body_bytes and result_bytes. It skips the other limits.
func (r *Reader) channel(c *Channel) error {
	err := r.members("channel", func(n string) (ok bool, err error) {
		switch n {
		case "token":
			c.Token, ok, err = r.p.str()
			ok = ok && validToken(c.Token)
		case "id":
			c.ID, ok, err = r.p.str()
			ok = ok && validChannelID(c.ID)
		case "limits":
			ok, err = true, r.members("limits", func(n string) (ok bool, err error) {
				switch n {
				case "request_body_bytes":
					c.Limits.RequestBodyBytes, ok, err = r.p.integer(1, math.MaxInt64)
				case "result_bytes":
					c.Limits.ResultBytes, ok, err = r.p.integer(1, math.MaxInt64)
				default:
					return r.skip()
				}
				return ok, err
			})
		case "mode":
			var s string
			s, ok, err = r.p.str()
			c.Mode = map[string]Mode{"write": ModeWrite, "read_only": ModeReadOnly}[s]
		case "epoch":
			c.Epoch, ok, err = r.p.integer(1, math.MaxInt64)
		default:
			return r.skip()
		}
		return ok, err
	})
	if err == nil && (c.Token == "" || c.ID == "" || c.Limits.RequestBodyBytes == 0 || c.Limits.ResultBytes == 0) {
		err = malformed("the channel lacks its token, its id or its limits")
	}
	return err
}

// validChannelID reports whether s is a channel identifier: 32 lowercase
// hexadecimal characters.
func validChannelID(s string) bool {
	if len(s) != 32 {
		return false
	}
	for i := range len(s) {
		if (s[i] < '0' || s[i] > '9') && (s[i] < 'a' || s[i] > 'f') {
			return false
		}
	}
	return true
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
