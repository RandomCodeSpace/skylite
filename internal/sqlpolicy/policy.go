// Package sqlpolicy restricts the SQL that clients may run on a SQLite
// connection. Decision record 0007 (docs/content/docs/decisions/0007-sql-policy.md)
// states the rules; this package applies them to an ncruces connection.
package sqlpolicy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// Limits are the limits a Policy applies to a connection. A zero field takes
// its default. The defaults suit tests; the server sets each field from its
// own configuration.
type Limits struct {
	MaxLength            int           // bytes in one string or BLOB, one row's record and one aggregate's result (SQLITE_LIMIT_LENGTH)
	MaxSQLLength         int           // bytes of SQL text given to one Run (SQLITE_LIMIT_SQL_LENGTH)
	MaxExprDepth         int           // depth of an expression tree, against which joined tables, composite-key pairs, nested subqueries and joined CTEs also count, and nesting of views, CTEs and subqueries (SQLITE_LIMIT_EXPR_DEPTH)
	MaxParserDepth       int           // depth of the parser's stack, which nesting in parentheses uses (SQLITE_LIMIT_PARSER_DEPTH)
	MaxCompoundSelect    int           // terms in a compound SELECT (SQLITE_LIMIT_COMPOUND_SELECT)
	MaxVDBEOps           int           // instructions in one compiled statement (SQLITE_LIMIT_VDBE_OP)
	MaxFunctionArgs      int           // arguments to one SQL function (SQLITE_LIMIT_FUNCTION_ARG)
	MaxLikePatternLength int           // bytes in a LIKE or GLOB pattern (SQLITE_LIMIT_LIKE_PATTERN_LENGTH)
	MaxVariableNumber    int           // highest parameter number (SQLITE_LIMIT_VARIABLE_NUMBER)
	MaxTriggerDepth      int           // nesting depth of triggers and foreign key actions (SQLITE_LIMIT_TRIGGER_DEPTH)
	MaxPageCount         int           // pages the main database may hold (PRAGMA max_page_count); zero leaves SQLite's
	Timeout              time.Duration // wall-clock time one Run may take
}

type limit struct {
	name          string
	id            sqlite3.LimitCategory
	value         *int
	def, min, max int // max is the bound compiled into SQLite, or a bound measured against the C stack
}

func (l *Limits) limits() []limit {
	return []limit{
		{"MaxLength", sqlite3.LIMIT_LENGTH, &l.MaxLength, 1 << 20, 30, 1_000_000_000}, // SQLite raises a lower value to 30
		{"MaxSQLLength", sqlite3.LIMIT_SQL_LENGTH, &l.MaxSQLLength, 1 << 20, 1, 1_000_000_000},
		// The next five bound recursion in the C code, whose stack is a fixed
		// 64 KiB in the ncruces build. They lower the chance of an overflow
		// and do not prevent one; New also limits a modelled cost of their
		// combination; see stackCost.
		{"MaxExprDepth", sqlite3.LIMIT_EXPR_DEPTH, &l.MaxExprDepth, 18, 3, 1000},
		// The parser's first 50 stack entries are static, so a lower limit has no effect.
		{"MaxParserDepth", sqlite3.LIMIT_PARSER_DEPTH, &l.MaxParserDepth, 100, 50, 2500},
		{"MaxCompoundSelect", sqlite3.LIMIT_COMPOUND_SELECT, &l.MaxCompoundSelect, 4, 1, 500},
		// Matching recurses once per wildcard; it overflowed from about 9,000 bytes.
		{"MaxLikePatternLength", sqlite3.LIMIT_LIKE_PATTERN_LENGTH, &l.MaxLikePatternLength, 4096, 1, 4096},
		{"MaxTriggerDepth", sqlite3.LIMIT_TRIGGER_DEPTH, &l.MaxTriggerDepth, 8, 1, 1000},
		{"MaxVDBEOps", sqlite3.LIMIT_VDBE_OP, &l.MaxVDBEOps, 250_000_000, 1, 250_000_000},
		{"MaxFunctionArgs", sqlite3.LIMIT_FUNCTION_ARG, &l.MaxFunctionArgs, 1000, 1, 1000},
		{"MaxVariableNumber", sqlite3.LIMIT_VARIABLE_NUMBER, &l.MaxVariableNumber, 32766, 1, 32766},
	}
}

const defaultTimeout = 5 * time.Second

// stackBudget and stackCost model how deep one statement can drive the C
// stack within the limits. The unit is what one term of a compound SELECT
// costs at one level of nesting. Measured on SQLite 3.54.1 in ncruces
// v0.37.0, a connection broke at about 180 units: nesting levels times terms,
// whether the levels were views, CTEs or subqueries in FROM, plus about 2.5
// units per level of trigger nesting. Views and CTEs nest to MaxExprDepth-2
// levels; subqueries in FROM nest to about MaxParserDepth/6.5. The budget is
// half of that. The model is a floor, not a guarantee: it counts one kind of
// nesting at a time, and statements within the limits that combine kinds
// still overflow the stack (record 0007, The C stack); Run then returns
// ErrBroken. Re-measure on any upgrade of the binding.
const stackBudget = 90

func stackCost(l Limits) int {
	levels := max(l.MaxExprDepth-2, l.MaxParserDepth/6)
	return l.MaxCompoundSelect*levels + 5*l.MaxTriggerDepth/2
}

// maxPageCount is SQLITE_MAX_PAGE_COUNT, SQLite's default.
const maxPageCount = 4294967294

// Policy is a validated set of limits. Create one with New.
type Policy struct {
	limits Limits
}

// New returns a Policy with l's limits, using the default for each zero field.
// It fails if a field is out of the range the SQLite build supports, or if
// the nesting limits together exceed the modelled stack cost, which is a
// floor: SQL within the limits can still overflow the stack. MaxSQLLength
// and MaxVDBEOps also bound the policy's own statements, so Apply, not New,
// fails when they are too low for them.
func New(l Limits) (*Policy, error) {
	for _, f := range l.limits() {
		switch {
		case *f.value == 0:
			*f.value = f.def
		case *f.value < f.min || *f.value > f.max:
			return nil, fmt.Errorf("sqlpolicy: %s is %d, want %d to %d", f.name, *f.value, f.min, f.max)
		}
	}
	if c := stackCost(l); c > stackBudget {
		return nil, fmt.Errorf("sqlpolicy: MaxCompoundSelect %d, MaxExprDepth %d, MaxParserDepth %d and MaxTriggerDepth %d together cost %d stack units, want at most %d",
			l.MaxCompoundSelect, l.MaxExprDepth, l.MaxParserDepth, l.MaxTriggerDepth, c, stackBudget)
	}
	if l.MaxPageCount < 0 || l.MaxPageCount > maxPageCount {
		return nil, fmt.Errorf("sqlpolicy: MaxPageCount is %d, want 0 to %d", l.MaxPageCount, maxPageCount)
	}
	switch {
	case l.Timeout == 0:
		l.Timeout = defaultTimeout
	case l.Timeout < 0:
		return nil, fmt.Errorf("sqlpolicy: Timeout is %v, want a positive duration", l.Timeout)
	}
	return &Policy{limits: l}, nil
}

// Conn is a SQLite connection with a Policy applied. Client SQL must run
// through Run, and the server's own SQL through Privileged; like the
// connection it wraps, a Conn is not safe for concurrent use.
type Conn struct {
	conn    *sqlite3.Conn
	limits  Limits
	refused Reason // first refusal by the authorizer since Run started
	inTxn   bool   // a transaction was open when Run started
	altered bool   // the authorizer has seen ALTER TABLE since Run started
	checks  bool   // the authorizer has seen sqlite_fail since Run started
	broken  bool   // the engine panicked, or Reset failed
}

// ErrBroken reports that the engine panicked, for example on a stack
// overflow, or that a reset failed, which leaves the connection in an
// unknown state. Whether a write transaction was open
// cannot be known; the engine must assume that one was, and discard the
// connection.
var ErrBroken = errors.New("sqlpolicy: connection unusable")

// Apply configures c for untrusted SQL: database configuration, temporary
// storage in memory, limits, the optional page count, and the authorizer.
// The engine calls Apply after its own set-up of c, and issues every later
// statement of its own through Privileged. Setting the authorizer expires
// statements prepared on c before Apply, so SQLite compiles them again under
// the policy if they are stepped.
func (p *Policy) Apply(c *sqlite3.Conn) (*Conn, error) {
	for _, cfg := range []struct {
		op   sqlite3.DBConfig
		want bool
	}{
		{sqlite3.DBCONFIG_DEFENSIVE, true},
		{sqlite3.DBCONFIG_TRUSTED_SCHEMA, false},
		{sqlite3.DBCONFIG_WRITABLE_SCHEMA, false},
		{sqlite3.DBCONFIG_ENABLE_LOAD_EXTENSION, false},
	} {
		got, err := c.Config(cfg.op, cfg.want)
		if err != nil {
			return nil, fmt.Errorf("sqlpolicy: db config %d: %w", cfg.op, err)
		}
		if got != cfg.want {
			return nil, fmt.Errorf("sqlpolicy: db config %d is %t, want %t", cfg.op, got, cfg.want)
		}
	}
	// The authorizer does not see the functions in the CHECK constraint or
	// generated column of ALTER TABLE ADD COLUMN, and trusted_schema off
	// stops only functions that are not innocuous. So every refused function,
	// at every number of arguments, is replaced by one that is not innocuous
	// and fails. The function list compiles to more instructions than a low
	// MaxVDBEOps allows, so this comes before the limits.
	type fn struct {
		name string
		narg int
	}
	var refused []fn
	if err := rows(c, "SELECT DISTINCT name, narg FROM pragma_function_list", func(s *sqlite3.Stmt) {
		if name := s.ColumnText(0); !allowedFunctions[name] && !alterFunctions[name] {
			refused = append(refused, fn{name, s.ColumnInt(1)})
		}
	}); err != nil {
		return nil, fmt.Errorf("sqlpolicy: function list: %w", err)
	}
	l := p.limits
	noAttach := 0
	for _, f := range append(l.limits(), limit{name: "attached databases", id: sqlite3.LIMIT_ATTACHED, value: &noAttach}) {
		c.Limit(f.id, *f.value)
		if got := c.Limit(f.id, -1); got != *f.value {
			return nil, fmt.Errorf("sqlpolicy: limit on %s is %d, want %d", f.name, got, *f.value)
		}
	}
	pc := &Conn{conn: c, limits: l}
	for _, f := range refused {
		if err := c.CreateFunction(f.name, f.narg, 0, pc.refuse); err != nil {
			return nil, fmt.Errorf("sqlpolicy: %s: %w", f.name, err)
		}
	}
	// The two-argument trims replace SQLite's, whose time grows with the
	// product of the lengths of their arguments.
	for name, ends := range map[string][2]bool{"trim": {true, true}, "ltrim": {true, false}, "rtrim": {false, true}} {
		if err := c.CreateFunction(name, 2, sqlite3.DETERMINISTIC|sqlite3.INNOCUOUS, pc.trim(ends[0], ends[1])); err != nil {
			return nil, fmt.Errorf("sqlpolicy: %s: %w", name, err)
		}
	}
	// Privileged installs the authorizer when it returns, on every path.
	err := pc.Privileged(func(c *sqlite3.Conn) error {
		// Temporary tables, indices and statement journals stay in the
		// connection's memory instead of unnamed files in the temporary
		// directory.
		if err := c.Exec("PRAGMA temp_store = MEMORY"); err != nil {
			return fmt.Errorf("temp_store: %w", err)
		}
		if l.MaxPageCount == 0 {
			return nil
		}
		// SQLite keeps the current size if the database is already larger.
		got, err := pragmaInt(c, fmt.Sprintf("PRAGMA main.max_page_count = %d", l.MaxPageCount))
		if err == nil && got != l.MaxPageCount {
			err = fmt.Errorf("max_page_count is %d, want %d", got, l.MaxPageCount)
		}
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("sqlpolicy: %w", err)
	}
	return pc, nil
}

// Run compiles the one statement in sql and runs it, interrupting it when
// the policy's timeout or ctx ends. fn binds parameters and steps the
// statement; a nil fn steps it to completion. Run closes the statement after
// fn returns.
//
// Run refuses, before anything is compiled, SQL longer than MaxSQLLength or
// holding a NUL byte; and, before any step, SQL that holds no statement or
// anything after the first statement other than white space, comments and
// the statement's own semicolon. A refusal by the policy, including the
// timeout and the memory limit, is returned as *Error; any other error comes
// from SQLite or from fn. After a panic in the engine, this Run and every
// later one return an error that wraps ErrBroken, or *Error with
// ReasonMemory for the first. A panic in fn's own code is not recovered.
func (c *Conn) Run(ctx context.Context, sql string, fn func(*sqlite3.Stmt) error) (err error) {
	switch {
	case c.broken:
		return ErrBroken
	case len(sql) > c.limits.MaxSQLLength:
		return &Error{Reason: ReasonSQLLength}
	case strings.IndexByte(sql, 0) >= 0:
		return &Error{Reason: ReasonNUL}
	}
	ctx, cancel := context.WithTimeoutCause(ctx, c.limits.Timeout, errTimeLimit)
	defer cancel()
	defer c.conn.SetInterrupt(c.conn.SetInterrupt(ctx))
	defer c.recoverEngine(&err)
	c.refused, c.altered, c.checks, c.inTxn = "", false, false, !c.conn.GetAutocommit()

	stmt, tail, err := c.conn.Prepare(sql)
	switch {
	case err != nil:
		return c.classify(ctx, err)
	case stmt == nil:
		return &Error{Reason: ReasonStatementCount}
	case !blank(tail):
		return errors.Join(&Error{Reason: ReasonStatementCount}, stmt.Close())
	case c.checks:
		// ALTER TABLE ADD CONSTRAINT, ADD CHECK and SET NOT NULL check the
		// existing rows with SQL that prefers SQLite's built-in functions,
		// so its trim, not the policy's.
		return errors.Join(&Error{Reason: ReasonStatement}, stmt.Close())
	}
	if err := run(stmt, fn); err != nil {
		return c.classify(ctx, err)
	}
	return nil
}

// Privileged runs fn with the authorizer suspended, for the server's own
// statements: BEGIN, COMMIT, ROLLBACK and its own pragmas. It restores the
// authorizer when fn returns or panics. fn must close every statement it
// prepares. A client's triggers and views that fn's statements reach run
// with no authorizer and no time limit; see the record's requirements.
func (c *Conn) Privileged(fn func(*sqlite3.Conn) error) (err error) {
	if c.broken {
		return ErrBroken
	}
	defer c.recoverEngine(&err)
	if err := c.conn.SetAuthorizer(nil); err != nil {
		return err
	}
	defer func() {
		if aerr := c.conn.SetAuthorizer(c.authorize); aerr != nil {
			c.broken = true
			err = fmt.Errorf("%w: authorizer: %w", ErrBroken, aerr)
		}
	}()
	return fn(c.conn)
}

// Reset returns everything a client can change on the connection to the
// policy's defaults: it rolls back an open transaction, drops every object in
// the temp schema and empties its sqlite_sequence, turns foreign key
// enforcement on and clears the last inserted row id. It is interrupted when
// the policy's timeout or ctx ends. If it fails, the connection is broken.
func (c *Conn) Reset(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, c.limits.Timeout)
	defer cancel()
	err := c.Privileged(func(c *sqlite3.Conn) error {
		// Its time grows with the square of the number of temp objects.
		defer c.SetInterrupt(c.SetInterrupt(ctx))
		if !c.GetAutocommit() {
			if err := c.Exec("ROLLBACK"); err != nil {
				return err
			}
		}
		// With foreign keys on, dropping a parent table deletes its rows first,
		// which a child table's references refuse.
		drops := []string{"PRAGMA foreign_keys = OFF"}
		left := "SELECT count(*) FROM temp.sqlite_schema WHERE name <> 'sqlite_sequence'"
		if err := rows(c, `SELECT type, name FROM temp.sqlite_schema WHERE type IN ('table', 'view', 'trigger')`, func(s *sqlite3.Stmt) {
			if name := s.ColumnText(1); name != "sqlite_sequence" {
				drops = append(drops, fmt.Sprintf(`DROP %s IF EXISTS temp."%s"`, s.ColumnText(0), strings.ReplaceAll(name, `"`, `""`)))
			} else {
				// SQLite does not let it be dropped, and a client can write to it.
				drops = append(drops, "DELETE FROM temp.sqlite_sequence")
				left = "SELECT (" + left + ") + (SELECT count(*) FROM temp.sqlite_sequence)"
			}
		}); err != nil {
			return err
		}
		drops = append(drops, "PRAGMA foreign_keys = ON")
		for _, d := range drops {
			if err := c.Exec(d); err != nil {
				return err
			}
		}
		c.SetLastInsertRowID(0)
		n, err := pragmaInt(c, left)
		if err == nil && n != 0 {
			err = fmt.Errorf("%d temp objects or sequence rows left", n)
		}
		if err != nil {
			return err
		}
		on, err := pragmaInt(c, "PRAGMA foreign_keys")
		if err == nil && on != 1 {
			err = fmt.Errorf("foreign_keys is %d", on)
		}
		return err
	})
	if err != nil && !errors.Is(err, ErrBroken) {
		c.broken = true
		err = fmt.Errorf("%w: reset: %w", ErrBroken, err)
	}
	return err
}

// Close closes the connection. After ErrBroken, SQLite may be unable to
// close it, and its files are then released only when Go finalizes them,
// which does not release its locks; see the record.
func (c *Conn) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: close: %v", ErrBroken, r)
		}
	}()
	return c.conn.Close()
}

// recoverEngine turns a panic that the engine raised into an error and
// marks the connection broken. ncruces panics with an "out of memory" error
// when SQLite reports SQLITE_NOMEM, and with a runtime error when the
// translated C code overflows its stack; either can leave the engine's state
// half-updated. A panic raised anywhere else is passed on.
func (c *Conn) recoverEngine(err *error) {
	r := recover()
	if r == nil {
		return
	}
	if !fromEngine() {
		panic(r)
	}
	c.broken = true
	// shortcut: the out-of-memory value's type is internal to ncruces, so it is matched by message; recheck on upgrade.
	if fmt.Sprint(r) == "sqlite3: out of memory" {
		*err = &Error{Reason: ReasonMemory}
	} else {
		*err = fmt.Errorf("%w: engine panic: %v", ErrBroken, r)
	}
}

// fromEngine reports whether the panic being recovered was raised in the
// ncruces packages, which include the translated SQLite code. It must be
// called from the deferred function that recovers.
func fromEngine() bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs)])
	panicking := false
	for {
		f, more := frames.Next()
		switch {
		case f.Function == "runtime.gopanic":
			panicking = true
		case panicking && !strings.HasPrefix(f.Function, "runtime."):
			return strings.HasPrefix(f.Function, "github.com/ncruces/go-sqlite3")
		}
		if !more {
			return false
		}
	}
}

// maxTrimChars bounds the character set of a two-argument trim, so that
// it takes time linear in the length of its first argument.
const maxTrimChars = 64

// trim is SQLite's trimFunc (func.c) for two arguments, which compares each
// end of the input with each character of the set in turn, refusing a set of
// more than maxTrimChars characters.
func (c *Conn) trim(left, right bool) sqlite3.ScalarFunction {
	return func(ctx sqlite3.Context, arg ...sqlite3.Value) {
		if arg[0].Type() == sqlite3.NULL || arg[1].Type() == sqlite3.NULL {
			return
		}
		in, set := arg[0].RawText(), arg[1].RawText()
		if i := bytes.IndexByte(set, 0); i >= 0 {
			set = set[:i] // SQLite reads the set as a C string
		}
		var chars [][]byte
		for len(set) > 0 {
			n := 1 // SQLITE_SKIP_UTF8
			if set[0] >= 0xc0 {
				for n < len(set) && set[n]&0xc0 == 0x80 {
					n++
				}
			}
			if chars = append(chars, set[:n]); len(chars) > maxTrimChars {
				c.refused = ReasonFunction
				ctx.ResultError(&Error{Reason: ReasonFunction})
				return
			}
			set = set[n:]
		}
		for left && len(in) > 0 {
			i := slices.IndexFunc(chars, func(ch []byte) bool { return bytes.HasPrefix(in, ch) })
			if i < 0 {
				break
			}
			in = in[len(chars[i]):]
		}
		for right && len(in) > 0 {
			i := slices.IndexFunc(chars, func(ch []byte) bool { return bytes.HasSuffix(in, ch) })
			if i < 0 {
				break
			}
			in = in[:len(in)-len(chars[i])]
		}
		ctx.ResultRawText(in)
	}
}

// refuse stands in for a function the policy refuses.
func (c *Conn) refuse(ctx sqlite3.Context, _ ...sqlite3.Value) {
	c.refused = ReasonFunction
	ctx.ResultError(&Error{Reason: ReasonFunction})
}

func run(stmt *sqlite3.Stmt, fn func(*sqlite3.Stmt) error) (err error) {
	defer func() {
		if cerr := stmt.Close(); err == nil {
			err = cerr
		}
	}()
	if fn == nil {
		return stmt.Exec()
	}
	return fn(stmt)
}

// rows calls f for each row of sql.
func rows(c *sqlite3.Conn, sql string, f func(*sqlite3.Stmt)) error {
	s, _, err := c.Prepare(sql)
	if err != nil {
		return err
	}
	for s.Step() {
		f(s)
	}
	return errors.Join(s.Err(), s.Close())
}

func pragmaInt(c *sqlite3.Conn, sql string) (n int, err error) {
	err = rows(c, sql, func(s *sqlite3.Stmt) { n = s.ColumnInt(0) })
	return n, err
}

// blank reports whether s holds only white space and comments, as SQLite's
// tokenizer reads them.
func blank(s string) bool {
	for s != "" {
		switch {
		case strings.IndexByte(" \t\n\f\r", s[0]) >= 0:
			s = s[1:]
		case strings.HasPrefix(s, "--"):
			_, s, _ = strings.Cut(s, "\n")
		case strings.HasPrefix(s, "/*"):
			_, s, _ = strings.Cut(s[2:], "*/")
		default:
			return false
		}
	}
	return true
}

var errTimeLimit = errors.New("sqlpolicy: time limit")

func (c *Conn) classify(ctx context.Context, err error) error {
	switch {
	case c.refused != "":
		return &Error{Reason: c.refused}
	case errors.Is(err, sqlite3.INTERRUPT) && errors.Is(context.Cause(ctx), errTimeLimit):
		return &Error{Reason: ReasonTimeLimit}
	}
	return err
}

func (c *Conn) authorize(action sqlite3.AuthorizerActionCode, name3rd, name4th, _, _ string) sqlite3.AuthorizerReturnCode {
	r := c.decide(action, name3rd, name4th)
	if r == "" {
		return sqlite3.AUTH_OK
	}
	if c.refused == "" {
		c.refused = r
	}
	return sqlite3.AUTH_DENY
}

// decide is the authorizer's rule table. SQLite calls it while it compiles a
// statement, and again for statements it compiles while one runs (a pragma
// table-valued function, the ATTACH inside VACUUM, and the SQL of ALTER
// TABLE).
func (c *Conn) decide(action sqlite3.AuthorizerActionCode, name3rd, name4th string) Reason {
	switch action {
	case sqlite3.AUTH_SELECT, sqlite3.AUTH_READ, sqlite3.AUTH_RECURSIVE,
		sqlite3.AUTH_INSERT, sqlite3.AUTH_UPDATE, sqlite3.AUTH_DELETE,
		sqlite3.AUTH_CREATE_TABLE, sqlite3.AUTH_CREATE_INDEX, sqlite3.AUTH_CREATE_VIEW, sqlite3.AUTH_CREATE_TRIGGER,
		sqlite3.AUTH_CREATE_TEMP_TABLE, sqlite3.AUTH_CREATE_TEMP_INDEX, sqlite3.AUTH_CREATE_TEMP_VIEW, sqlite3.AUTH_CREATE_TEMP_TRIGGER,
		sqlite3.AUTH_DROP_TABLE, sqlite3.AUTH_DROP_INDEX, sqlite3.AUTH_DROP_VIEW, sqlite3.AUTH_DROP_TRIGGER,
		sqlite3.AUTH_DROP_TEMP_TABLE, sqlite3.AUTH_DROP_TEMP_INDEX, sqlite3.AUTH_DROP_TEMP_VIEW, sqlite3.AUTH_DROP_TEMP_TRIGGER,
		sqlite3.AUTH_REINDEX, sqlite3.AUTH_ANALYZE:
		return ""
	case sqlite3.AUTH_ALTER_TABLE:
		c.altered = true
		return ""
	case sqlite3.AUTH_TRANSACTION:
		return ReasonTransaction
	case sqlite3.AUTH_SAVEPOINT:
		// Outside a transaction, SAVEPOINT would begin one.
		if c.inTxn {
			return ""
		}
		return ReasonTransaction
	case sqlite3.AUTH_PRAGMA:
		name := strings.ToLower(name3rd)
		if allowedPragmas[name] || name == "quick_check" && c.altered {
			return ""
		}
		return ReasonPragma
	case sqlite3.AUTH_FUNCTION:
		name := strings.ToLower(name4th)
		// Only the forms of ALTER TABLE that check existing rows compile it;
		// Run refuses them before any step.
		c.checks = c.checks || name == "sqlite_fail"
		if allowedFunctions[name] || alterFunctions[name] {
			return ""
		}
		return ReasonFunction
	case sqlite3.AUTH_ATTACH:
		return ReasonAttach
	case sqlite3.AUTH_DETACH:
		return ReasonDetach
	}
	return ReasonStatement
}

// allowedPragmas are the read-only schema introspection pragmas that JDBC
// metadata and GORM's migrator issue, and foreign_keys, a session setting
// clients may read and change. quick_check, which SQLite runs itself in
// ALTER TABLE ADD COLUMN on a STRICT table or one with CHECK constraints, is
// allowed only after the authorizer has seen ALTER TABLE in the same Run.
// Every other pragma is refused.
var allowedPragmas = set(
	"table_info", "table_xinfo", "index_list", "index_info", "foreign_key_list", "foreign_keys",
)

// allowedFunctions are the SQL functions registered by ncruces v0.37.0 that
// compute only on their arguments, in time close to linear in their size or,
// for replace and instr, bounded by MaxLength. Every other function is
// refused, including sqlite_log, sqlite_compileoption_get,
// sqlite_compileoption_used, sqlite_source_id, match, regexp, regexpi, unhex,
// json_patch, jsonb_patch and the decimal functions, and any function
// registered later; Apply replaces those registered when it runs. Two-argument
// trim, ltrim and rtrim are the policy's own.
var allowedFunctions = set(
	// Core scalar functions.
	"abs", "changes", "char", "coalesce", "concat", "concat_ws", "format", "glob", "hex", "if", "ifnull", "iif",
	"instr", "last_insert_rowid", "length", "like", "likelihood", "likely", "lower", "ltrim", "max", "min", "nullif",
	"octet_length", "printf", "quote", "random", "randomblob", "replace", "round", "rtrim", "sign", "soundex",
	"sqlite_version", "substr", "substring", "subtype", "total_changes", "trim", "typeof", "unicode",
	"unistr", "unistr_quote", "unlikely", "upper", "zeroblob",
	// Aggregate and window functions.
	"avg", "count", "group_concat", "string_agg", "sum", "total",
	"cume_dist", "dense_rank", "first_value", "lag", "last_value", "lead", "nth_value", "ntile", "percent_rank",
	"rank", "row_number",
	// Date and time.
	"date", "time", "datetime", "julianday", "unixepoch", "strftime", "timediff",
	"current_date", "current_time", "current_timestamp",
	// Math.
	"acos", "acosh", "asin", "asinh", "atan", "atan2", "atanh", "ceil", "ceiling", "cos", "cosh", "degrees", "exp",
	"floor", "ln", "log", "log10", "log2", "mod", "pi", "pow", "power", "radians", "sin", "sinh", "sqrt", "tan",
	"tanh", "trunc",
	// JSON.
	"->", "->>", "json", "json_array", "json_array_insert", "json_array_length", "json_error_position",
	"json_extract", "json_group_array", "json_group_object", "json_insert", "json_object",
	"json_pretty", "json_quote", "json_remove", "json_replace", "json_set", "json_type", "json_valid",
	"jsonb", "jsonb_array", "jsonb_array_insert", "jsonb_extract", "jsonb_group_array", "jsonb_group_object",
	"jsonb_insert", "jsonb_object", "jsonb_remove", "jsonb_replace", "jsonb_set",
	// Extensions that ncruces registers on every connection.
	"base64", "ieee754", "ieee754_exponent", "ieee754_from_blob", "ieee754_from_int", "ieee754_inc",
	"ieee754_mantissa", "ieee754_to_blob", "ieee754_to_int",
)

// alterFunctions are SQLite's internal functions for ALTER TABLE, which it
// calls through SQL that it compiles itself. The authorizer sees them, but
// SQLite resolves them only in that SQL, so a client cannot call them.
var alterFunctions = set(
	"sqlite_rename_column", "sqlite_rename_table", "sqlite_rename_test", "sqlite_drop_column",
	"sqlite_rename_quotefix", "sqlite_drop_constraint", "sqlite_fail", "sqlite_add_constraint",
	"sqlite_find_constraint",
)

func set(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// Reason names the rule that refused a statement. Its values are stable.
type Reason string

// Reasons.
const (
	ReasonAttach         Reason = "attach"          // ATTACH, and VACUUM, which attaches its output
	ReasonDetach         Reason = "detach"          // DETACH
	ReasonTransaction    Reason = "transaction"     // BEGIN, COMMIT, END, ROLLBACK; SAVEPOINT, RELEASE and ROLLBACK TO outside a transaction
	ReasonPragma         Reason = "pragma"          // a pragma outside the allowlist, in any form
	ReasonFunction       Reason = "function"        // an SQL function outside the allowlist
	ReasonStatement      Reason = "statement"       // any other kind of statement, such as CREATE VIRTUAL TABLE, or an ALTER TABLE that checks existing rows
	ReasonStatementCount Reason = "statement_count" // no statement, or text after the first one
	ReasonNUL            Reason = "nul"             // a NUL byte in the SQL
	ReasonSQLLength      Reason = "sql_length"      // SQL longer than MaxSQLLength
	ReasonTimeLimit      Reason = "time_limit"      // the statement ran past the policy's timeout
	ReasonMemory         Reason = "memory"          // the connection's memory; the Conn is then broken
)

// Error is a refusal by the policy. It carries no SQL text, which can hold
// secrets.
type Error struct {
	Reason Reason
}

func (e *Error) Error() string {
	return "sqlpolicy: refused (" + string(e.Reason) + ")"
}
