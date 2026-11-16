package wire

import (
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
)

// DefaultFrameBytes is the default frame size: the largest frame the server
// encodes, except a row, or a results or params frame of one entry, that is
// larger by itself.
const DefaultFrameBytes = 1 << 20

// Limits is the limits object. Every member is always present.
type Limits struct {
	RequestBodyBytes     int64
	HeaderBlockBytes     int64
	BatchEntries         int64
	FrameEntries         int64
	ResultBytes          int64
	HeldResultBytes      int64
	TerminalFrameBytes   int64
	ErrorMessageBytes    int64
	RetainedRequestBytes int64
	ResolveBound         int64
	StatementMS          int64
	ResponseMS           int64
	KeepaliveMS          int64
	LockWaitMS           int64
	CatchUpMS            int64
	ChannelIdleMS        int64
	WriteTxnIdleMS       int64
	WriteTxnLifetimeMS   int64
	ReadTxnIdleMS        int64
	ReadTxnLifetimeMS    int64
}

// DefaultLimits returns the defaults of ADR 0005 decision 9.
func DefaultLimits() Limits {
	return Limits{
		RequestBodyBytes:     16 << 20,
		HeaderBlockBytes:     64 << 10,
		BatchEntries:         10000,
		FrameEntries:         1024,
		ResultBytes:          64 << 20,
		HeldResultBytes:      1 << 20,
		TerminalFrameBytes:   4 << 10,
		ErrorMessageBytes:    1024,
		RetainedRequestBytes: 64 << 10,
		ResolveBound:         100000,
		StatementMS:          30000,
		ResponseMS:           10000,
		KeepaliveMS:          10000,
		LockWaitMS:           5000,
		CatchUpMS:            500,
		ChannelIdleMS:        600000,
		WriteTxnIdleMS:       30000,
		WriteTxnLifetimeMS:   300000,
		ReadTxnIdleMS:        30000,
		ReadTxnLifetimeMS:    120000,
	}
}

// The largest error message bound, batch limit and entries per frame a
// server may configure: ADR 0005's terminal frame bounds hold at them.
const (
	maxErrorMessageBytes = 1024
	maxBatchEntries      = 10000
	maxFrameEntries      = 1024
)

// Validate checks l against the limits object's ranges: every member from
// 1 to 2^63-1, or to 2^31-1 in milliseconds, and the error message bound
// (at least 2 bytes, the empty message), the batch limit and the entries
// per frame at most their defaults, which are maxima.
func (l *Limits) Validate() error {
	for _, m := range l.members() {
		hi := int64(math.MaxInt64)
		switch m.name {
		case "error_message_bytes":
			if m.v < 2 {
				return fmt.Errorf("wire: error_message_bytes %d cannot hold an empty message", m.v)
			}
			hi = maxErrorMessageBytes
		case "batch_entries":
			hi = maxBatchEntries
		case "frame_entries":
			hi = maxFrameEntries
		}
		if strings.HasSuffix(m.name, "_ms") {
			hi = math.MaxInt32
		}
		if m.v < 1 || m.v > hi {
			return fmt.Errorf("wire: %s %d is outside 1 to %d", m.name, m.v, hi)
		}
	}
	return nil
}

type limit struct {
	name string
	v    int64
}

func (l *Limits) members() []limit {
	return []limit{
		{"request_body_bytes", l.RequestBodyBytes},
		{"header_block_bytes", l.HeaderBlockBytes},
		{"batch_entries", l.BatchEntries},
		{"frame_entries", l.FrameEntries},
		{"result_bytes", l.ResultBytes},
		{"held_result_bytes", l.HeldResultBytes},
		{"terminal_frame_bytes", l.TerminalFrameBytes},
		{"error_message_bytes", l.ErrorMessageBytes},
		{"retained_request_bytes", l.RetainedRequestBytes},
		{"resolve_bound", l.ResolveBound},
		{"statement_ms", l.StatementMS},
		{"response_ms", l.ResponseMS},
		{"keepalive_ms", l.KeepaliveMS},
		{"lock_wait_ms", l.LockWaitMS},
		{"catch_up_ms", l.CatchUpMS},
		{"channel_idle_ms", l.ChannelIdleMS},
		{"write_txn_idle_ms", l.WriteTxnIdleMS},
		{"write_txn_lifetime_ms", l.WriteTxnLifetimeMS},
		{"read_txn_idle_ms", l.ReadTxnIdleMS},
		{"read_txn_lifetime_ms", l.ReadTxnLifetimeMS},
	}
}

func (l *Limits) append(b []byte) []byte {
	o := object{b: append(b, '{')}
	for _, m := range l.members() {
		o.int(m.name, m.v)
	}
	return append(o.b, '}')
}

// DoneKind selects the fields a done frame carries, one per row of ADR
// 0005's table of done fields.
type DoneKind uint8

// The done kinds.
const (
	DoneQuery           DoneKind = iota + 1 // txn, read_only, rows_affected, last_insert_rowid
	DoneDescribe                            // txn, param_count
	DoneOpen                                // txn, channel
	DoneBegin                               // seq, txn_state, txn: a begin without execute
	DoneExecute                             // seq, txn_state, txn, read_only, rows_affected, last_insert_rowid: execute, or begin with execute
	DoneBatch                               // seq, txn_state, txn, rows_affected, last_insert_rowid, entries
	DoneChannelDescribe                     // seq, txn_state, txn, param_count
	DoneCommit                              // seq, txn_state, txn, committed, wrote
	DoneRollback                            // seq, txn_state, txn: rollback and ping
	DoneCancel                              // seq, txn_state, outcome
	DoneClose                               // last_seq, and outcome unless last_seq is 0
	DoneResolve                             // resolution, txn
	DoneStatus                              // txn, epoch, sqlite_version, limits
	DonePut                                 // txn, epoch
)

// Resolution is a resolve answer.
type Resolution string

// The resolutions.
const (
	ResolutionCommitted    Resolution = "committed"
	ResolutionNotCommitted Resolution = "not_committed"
	ResolutionPending      Resolution = "pending"
	ResolutionUnknown      Resolution = "unknown"
)

// Done is a done frame. Kind decides which fields are written; every field
// it lists is written, zero or not.
type Done struct {
	Kind            DoneKind
	Seq             int64
	Txn             int64
	TxnState        TxnState
	ReadOnly        bool
	RowsAffected    int64
	LastInsertRowID int64
	Entries         int64
	ParamCount      int64
	Wrote           bool
	DataOmitted     bool // written only when true, on a sequenced request's replay
	Channel         *Channel
	LastSeq         int64
	Outcome         *Receipt
	Resolution      Resolution
	Epoch           int64
	SQLiteVersion   string
	Limits          *Limits
}

// Channel is an open answer's channel object. Epoch is written for a write
// channel only.
type Channel struct {
	Token  string
	ID     string
	Mode   Mode
	Epoch  int64
	Limits Limits
}

// Receipt is a sequenced request's recorded outcome: Done for a success, of
// a sequenced kind, or Error for a failure.
type Receipt struct {
	Done  *Done
	Error *Error
}

// Column is one entry of a columns frame. An empty string is written as null.
type Column struct {
	Name     string
	DeclType string
	Database string
	Table    string
	Origin   string
}

// Result is one entry of a results frame.
type Result struct {
	RowsAffected    int64
	LastInsertRowID int64
}

// Writer writes a response body: frames in the order ADR 0005's grammar
// allows, ending with one terminal frame. It is not safe for concurrent use.
type Writer struct {
	w          io.Writer
	frameBytes int
	limits     Limits
	buf        []byte
	n          int64 // bytes written before the terminal frame
	state      int
	columns    int
	results    int64 // index of the next results entry
	params     int64 // index of the next params name
}

// Writer states.
const (
	stateStart   = iota // keepalives so far
	stateRows           // after columns
	stateResults        // after results
	stateParams         // after params
	stateEnded          // after the terminal frame
)

var (
	errEnded   = errors.New("wire: the response has ended")
	errOrder   = errors.New("wire: frame out of order")
	errColumns = errors.New("wire: a row must have one value per column")
)

// NewWriter returns a Writer that writes each frame to w with one Write
// call. frameBytes is the frame size; l supplies the entries per frame, the
// result size, which counts every byte before the terminal frame, the
// terminal frame reservation and the error message bound. The held-result
// bound, which counts rows and results frames only, is the caller's to
// apply. NewWriter refuses a frame size below 1 and limits that Validate
// refuses.
func NewWriter(w io.Writer, frameBytes int, l Limits) (*Writer, error) {
	if frameBytes < 1 {
		return nil, fmt.Errorf("wire: frame size %d is below 1", frameBytes)
	}
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return &Writer{w: w, frameBytes: frameBytes, limits: l}, nil
}

// Written returns the bytes written before the terminal frame.
func (w *Writer) Written() int64 { return w.n }

// emit writes one frame. A frame before the terminal one that would take the
// response past the result size is not written: emit returns a
// result_too_large *Error and the writer stays usable for the terminal frame.
func (w *Writer) emit(frame []byte, terminal bool) error {
	switch {
	case w.state == stateEnded:
		return errEnded
	case terminal && int64(len(frame)) > w.limits.TerminalFrameBytes:
		return fmt.Errorf("wire: terminal frame of %d bytes exceeds its reservation", len(frame))
	case !terminal && w.n+int64(len(frame)) > w.limits.ResultBytes:
		return NewError(CodeResultTooLarge, fmt.Sprintf("the result exceeds %d bytes", w.limits.ResultBytes))
	}
	if _, err := w.w.Write(frame); err != nil {
		return err
	}
	if terminal {
		w.state = stateEnded
	} else {
		w.n += int64(len(frame))
	}
	return nil
}

// Keepalive writes a keepalive frame.
func (w *Writer) Keepalive() error {
	return w.emit([]byte("{\"keepalive\":{}}\n"), false)
}

// Columns writes a columns frame, which must come first. A columns frame
// above the frame size is refused with result_too_large.
func (w *Writer) Columns(cols []Column) error {
	switch w.state {
	case stateStart:
	case stateEnded:
		return errEnded
	default:
		return errOrder
	}
	b := append(w.buf[:0], `{"columns":[`...)
	for i, c := range cols {
		if i > 0 {
			b = append(b, ',')
		}
		o := object{b: append(b, '{')}
		o.str("name", c.Name)
		o.nullStr("decltype", c.DeclType)
		o.nullStr("database", c.Database)
		o.nullStr("table", c.Table)
		o.nullStr("origin", c.Origin)
		b = append(o.b, '}')
	}
	b = append(b, "]}\n"...)
	w.buf = b
	if len(b) > w.frameBytes {
		return NewError(CodeResultTooLarge, "the result's columns exceed the frame size")
	}
	if err := w.emit(b, false); err != nil {
		return err
	}
	w.state, w.columns = stateRows, len(cols)
	return nil
}

// Row writes a row, which must follow the columns frame and have one value
// per column. A row is never split, whatever its size.
func (w *Writer) Row(values []any) error {
	switch w.state {
	case stateRows:
	case stateEnded:
		return errEnded
	default:
		return errOrder
	}
	if len(values) != w.columns {
		return errColumns
	}
	b := append(w.buf[:0], '[')
	for i, v := range values {
		if i > 0 {
			b = append(b, ',')
		}
		var err error
		if b, err = AppendValue(b, v); err != nil {
			return err
		}
	}
	b = append(b, "]\n"...)
	w.buf = b
	return w.emit(b, false)
}

// Results writes a batch's results entries, continuing from the entries
// already written, in frames of at most FrameEntries entries that end before
// an entry that would take them past the frame size. Results may not follow
// params.
func (w *Writer) Results(entries []Result) error {
	if w.state == stateParams {
		return errOrder
	}
	return w.entries(stateResults, `{"results":{"from":`, `,"entries":[`, &w.results, len(entries), func(b []byte, i int) []byte {
		o := object{b: append(b, '{')}
		o.int("rows_affected", entries[i].RowsAffected)
		o.int("last_insert_rowid", entries[i].LastInsertRowID)
		return append(o.b, '}')
	})
}

// Params writes a describe answer's parameter names, continuing from those
// already written, split as Results splits. They must follow the columns
// frame, which a describe answer always has. An empty name is written as
// null.
func (w *Writer) Params(names []string) error {
	if w.state != stateRows && w.state != stateParams && w.state != stateEnded {
		return errOrder
	}
	return w.entries(stateParams, `{"params":{"from":`, `,"names":[`, &w.params, len(names), func(b []byte, i int) []byte {
		return appendNullStr(b, names[i])
	})
}

func (w *Writer) entries(state int, head, key string, from *int64, n int, entry func([]byte, int) []byte) error {
	if w.state == stateEnded {
		return errEnded
	}
	const tail = "]}}\n"
	for i := 0; i < n; {
		b := append(w.buf[:0], head...)
		b = strconv.AppendInt(b, *from, 10)
		b = append(b, key...)
		start := i
		for i < n && int64(i-start) < w.limits.FrameEntries {
			mark := len(b)
			if i > start {
				b = append(b, ',')
			}
			b = entry(b, i)
			if i > start && len(b)+len(tail) > w.frameBytes {
				b = b[:mark]
				break
			}
			i++
		}
		b = append(b, tail...)
		w.buf = b
		if err := w.emit(b, false); err != nil {
			return err
		}
		*from += int64(i - start)
		w.state = state
	}
	return nil
}

// Done writes a done frame, ending the response. It refuses, writing
// nothing, a Done whose kind is unknown, that lacks the Channel, Outcome or
// Limits its kind writes, whose outcome is not one Done of a sequenced
// request's kind or one Error, or that sets DataOmitted on an answer that
// is not a sequenced request's.
func (w *Writer) Done(d *Done) error {
	b := append(w.buf[:0], `{"done":`...)
	b, err := w.appendDone(b, d, false)
	if err != nil {
		return err
	}
	b = append(b, "}\n"...)
	w.buf = b
	return w.emit(b, true)
}

// Error writes an error frame, ending the response. The message is cut to
// the error message bound. It refuses, writing nothing, an error whose code
// is not in the list or whose request ID is not 1 to 64 characters from
// A-Z a-z 0-9 _ -.
func (w *Writer) Error(e *Error) error {
	b := append(w.buf[:0], `{"error":`...)
	b, err := w.appendError(b, e)
	if err != nil {
		return err
	}
	b = append(b, "}\n"...)
	w.buf = b
	return w.emit(b, true)
}

// appendDone appends d's object, or, for a receipt, d's fields after
// "seq" and "ok":true.
func (w *Writer) appendDone(b []byte, d *Done, receipt bool) ([]byte, error) {
	if d == nil {
		return b, errors.New("wire: no done fields")
	}
	sequenced := false // the answer of a sequenced request, which a receipt records
	switch d.Kind {
	case DoneBegin, DoneExecute, DoneBatch, DoneChannelDescribe, DoneCommit, DoneRollback:
		sequenced = true
	}
	switch {
	case receipt && !sequenced:
		return b, fmt.Errorf("wire: a receipt cannot hold done kind %d", d.Kind)
	case d.DataOmitted && !receipt && !sequenced:
		return b, fmt.Errorf("wire: data_omitted on done kind %d, which is never a replay", d.Kind)
	}
	o := object{b: append(b, '{')}
	if sequenced || d.Kind == DoneCancel {
		o.int("seq", d.Seq)
		if receipt {
			o.key("ok")
			o.b = append(o.b, "true"...)
		}
		o.str("txn_state", string(d.TxnState))
	}
	var err error
	switch d.Kind {
	case DoneQuery, DoneExecute:
		o.int("txn", d.Txn)
		o.bool("read_only", d.ReadOnly)
		o.int("rows_affected", d.RowsAffected)
		o.int("last_insert_rowid", d.LastInsertRowID)
	case DoneDescribe, DoneChannelDescribe:
		o.int("txn", d.Txn)
		o.int("param_count", d.ParamCount)
	case DoneOpen:
		c := d.Channel
		if c == nil {
			return b, errors.New("wire: an open answer needs its channel")
		}
		o.int("txn", d.Txn)
		o.key("channel")
		co := object{b: append(o.b, '{')}
		co.str("token", c.Token)
		co.str("id", c.ID)
		co.str("mode", c.Mode.String())
		if c.Mode == ModeWrite {
			co.int("epoch", c.Epoch)
		}
		co.key("limits")
		o.b = append(c.Limits.append(co.b), '}')
	case DoneBegin, DoneRollback:
		o.int("txn", d.Txn)
	case DoneBatch:
		o.int("txn", d.Txn)
		o.int("rows_affected", d.RowsAffected)
		o.int("last_insert_rowid", d.LastInsertRowID)
		o.int("entries", d.Entries)
	case DoneCommit:
		o.int("txn", d.Txn)
		o.bool("committed", true)
		o.bool("wrote", d.Wrote)
	case DoneCancel:
		o.key("outcome")
		o.b, err = w.appendReceipt(o.b, d.Outcome)
	case DoneClose:
		o.int("last_seq", d.LastSeq)
		if d.LastSeq != 0 {
			o.key("outcome")
			o.b, err = w.appendReceipt(o.b, d.Outcome)
		}
	case DoneResolve:
		o.str("resolution", string(d.Resolution))
		o.int("txn", d.Txn)
	case DoneStatus:
		if d.Limits == nil {
			return b, errors.New("wire: a status answer needs its limits")
		}
		o.int("txn", d.Txn)
		o.int("epoch", d.Epoch)
		o.str("sqlite_version", d.SQLiteVersion)
		o.key("limits")
		o.b = d.Limits.append(o.b)
	case DonePut:
		o.int("txn", d.Txn)
		o.int("epoch", d.Epoch)
	default:
		return b, fmt.Errorf("wire: unknown done kind %d", d.Kind)
	}
	if err != nil {
		return b, err
	}
	if d.DataOmitted && !receipt { // refused above unless sequenced
		o.bool("data_omitted", true)
	}
	return append(o.b, '}'), nil
}

// appendReceipt appends a receipt as embedded in a cancel or close answer.
func (w *Writer) appendReceipt(b []byte, r *Receipt) ([]byte, error) {
	if r == nil || (r.Done == nil) == (r.Error == nil) {
		return b, errors.New("wire: a receipt holds one done or one error")
	}
	if r.Error == nil {
		return w.appendDone(b, r.Done, true)
	}
	o := object{b: append(b, '{')}
	o.int("seq", r.Error.Seq)
	o.bool("ok", false)
	o.key("error")
	eb, err := w.appendError(o.b, r.Error)
	if err != nil {
		return b, err
	}
	return append(eb, '}'), nil
}

func (w *Writer) appendError(b []byte, e *Error) ([]byte, error) {
	switch {
	case e == nil:
		return b, errors.New("wire: no error")
	case !slices.Contains(codes, e.Code):
		return b, fmt.Errorf("wire: unknown error code %q", e.Code)
	case !validRequestID(e.RequestID):
		return b, fmt.Errorf("wire: request ID %q is not 1 to 64 characters from A-Z a-z 0-9 _ -", e.RequestID)
	}
	o := object{b: append(b, '{')}
	o.str("code", string(e.Code))
	o.bool("not_executed", e.NotExecuted)
	o.bool("final", e.Final)
	o.str("message", CutMessage(e.Message, int(w.limits.ErrorMessageBytes)))
	o.str("request_id", e.RequestID)
	o.optStr("channel_id", e.ChannelID)
	o.optInt("seq", e.Seq)
	o.optStr("txn_state", string(e.TxnState))
	o.optInt("sqlite_code", e.SQLiteCode)
	o.optInt("sqlite_extended_code", e.SQLiteExtendedCode)
	if e.BatchIndex != nil {
		o.int("batch_index", *e.BatchIndex)
	}
	o.optStr("cause", e.Cause)
	o.optInt("txn", e.Txn)
	o.optInt("epoch", e.Epoch)
	return append(o.b, '}'), nil
}

// validRequestID reports whether s is 1 to 64 characters from
// A-Z a-z 0-9 _ -.
func validRequestID(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for i := range len(s) {
		if c := s[i]; (c < 'A' || c > 'Z') && (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// object appends the members of a JSON object, with commas between them.
type object struct {
	b []byte
	n int
}

func (o *object) key(name string) {
	if o.n > 0 {
		o.b = append(o.b, ',')
	}
	o.n++
	o.b = append(append(append(o.b, '"'), name...), `":`...)
}

func (o *object) int(name string, v int64) {
	o.key(name)
	o.b = strconv.AppendInt(o.b, v, 10)
}

func (o *object) optInt(name string, v int64) {
	if v != 0 {
		o.int(name, v)
	}
}

func (o *object) bool(name string, v bool) {
	o.key(name)
	o.b = strconv.AppendBool(o.b, v)
}

// str appends a string member. Text the server writes outside values
// (names, messages) is diagnostic, so invalid UTF-8 in it becomes U+FFFD.
func (o *object) str(name, v string) {
	o.key(name)
	o.b = appendQuoted(o.b, v)
}

func (o *object) optStr(name, v string) {
	if v != "" {
		o.str(name, v)
	}
}

func (o *object) nullStr(name, v string) {
	o.key(name)
	o.b = appendNullStr(o.b, v)
}

func appendNullStr(b []byte, v string) []byte {
	if v == "" {
		return append(b, "null"...)
	}
	return appendQuoted(b, v)
}
