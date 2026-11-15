// Package sqlpolicy restricts the SQL that clients may run on a SQLite
// connection. Decision record 0007 (docs/content/docs/decisions/0007-sql-policy.md)
// states the rules; this package applies them to an ncruces connection.
package sqlpolicy

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/ncruces/go-sqlite3"
)

// Limits are the limits a Policy applies to a connection. A zero field takes
// its default.
type Limits struct {
	MaxLength            int           // bytes in one string or BLOB (SQLITE_LIMIT_LENGTH)
	MaxSQLLength         int           // bytes of SQL text in one statement (SQLITE_LIMIT_SQL_LENGTH)
	MaxExprDepth         int           // depth of an expression tree (SQLITE_LIMIT_EXPR_DEPTH)
	MaxParserDepth       int           // depth of the parser's stack, which nesting in parentheses uses (SQLITE_LIMIT_PARSER_DEPTH)
	MaxCompoundSelect    int           // terms in a compound SELECT (SQLITE_LIMIT_COMPOUND_SELECT)
	MaxVDBEOps           int           // instructions in one compiled statement (SQLITE_LIMIT_VDBE_OP)
	MaxFunctionArgs      int           // arguments to one SQL function (SQLITE_LIMIT_FUNCTION_ARG)
	MaxLikePatternLength int           // bytes in a LIKE or GLOB pattern (SQLITE_LIMIT_LIKE_PATTERN_LENGTH)
	MaxVariableNumber    int           // highest parameter number (SQLITE_LIMIT_VARIABLE_NUMBER)
	MaxTriggerDepth      int           // nesting depth of triggers (SQLITE_LIMIT_TRIGGER_DEPTH)
	Timeout              time.Duration // wall-clock time one Run may take
}

type limit struct {
	name          string
	id            sqlite3.LimitCategory
	value         *int
	def, min, max int // max is the bound compiled into SQLite, above which sqlite3_limit silently clamps
}

func (l *Limits) limits() []limit {
	return []limit{
		{"MaxLength", sqlite3.LIMIT_LENGTH, &l.MaxLength, 16 << 20, 1, 1_000_000_000},
		{"MaxSQLLength", sqlite3.LIMIT_SQL_LENGTH, &l.MaxSQLLength, 1 << 20, 1, 1_000_000_000},
		// The next five defaults are below SQLite's because, at SQLite's, one
		// statement can overflow the 64 KiB C stack of the ncruces build.
		{"MaxExprDepth", sqlite3.LIMIT_EXPR_DEPTH, &l.MaxExprDepth, 100, 1, 1000},
		// The parser's first 50 stack entries are static, so a lower limit has no effect.
		{"MaxParserDepth", sqlite3.LIMIT_PARSER_DEPTH, &l.MaxParserDepth, 200, 50, 2500},
		{"MaxCompoundSelect", sqlite3.LIMIT_COMPOUND_SELECT, &l.MaxCompoundSelect, 100, 1, 500},
		{"MaxLikePatternLength", sqlite3.LIMIT_LIKE_PATTERN_LENGTH, &l.MaxLikePatternLength, 4096, 1, 50000},
		{"MaxTriggerDepth", sqlite3.LIMIT_TRIGGER_DEPTH, &l.MaxTriggerDepth, 32, 1, 1000},
		{"MaxVDBEOps", sqlite3.LIMIT_VDBE_OP, &l.MaxVDBEOps, 1_000_000, 1, 250_000_000},
		{"MaxFunctionArgs", sqlite3.LIMIT_FUNCTION_ARG, &l.MaxFunctionArgs, 1000, 1, 1000},
		{"MaxVariableNumber", sqlite3.LIMIT_VARIABLE_NUMBER, &l.MaxVariableNumber, 32766, 1, 32766},
	}
}

const defaultTimeout = 5 * time.Second

// Policy is a validated set of limits. Create one with New.
type Policy struct {
	limits Limits
}

// New returns a Policy with l's limits, using the default for each zero field.
// It fails if a field is out of the range the SQLite build supports.
func New(l Limits) (*Policy, error) {
	for _, f := range l.limits() {
		switch {
		case *f.value == 0:
			*f.value = f.def
		case *f.value < f.min || *f.value > f.max:
			return nil, fmt.Errorf("sqlpolicy: %s is %d, want %d to %d", f.name, *f.value, f.min, f.max)
		}
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
// through Run; like the connection it wraps, a Conn is not safe for
// concurrent use.
type Conn struct {
	conn    *sqlite3.Conn
	timeout time.Duration
	refused Reason // first refusal by the authorizer since Run started
	broken  bool   // a Run recovered a panic from the engine
}

// ErrBroken reports that the engine panicked during a Run, which leaves the
// connection in an unknown state. Close the Conn and open a new connection.
var ErrBroken = errors.New("sqlpolicy: connection unusable after an engine panic")

// Apply configures c for untrusted SQL: database configuration, temporary
// storage in memory, limits, and the authorizer. Setting the authorizer
// expires statements prepared on c before Apply, so SQLite compiles them
// again under the policy if they are stepped.
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
	// Temporary tables, indices and statement journals stay in the
	// connection's memory instead of unnamed files in the temporary directory.
	if err := c.Exec("PRAGMA temp_store = MEMORY"); err != nil {
		return nil, fmt.Errorf("sqlpolicy: temp_store: %w", err)
	}
	l := p.limits
	noAttach := 0
	for _, f := range append(l.limits(), limit{name: "attached databases", id: sqlite3.LIMIT_ATTACHED, value: &noAttach}) {
		c.Limit(f.id, *f.value)
		if got := c.Limit(f.id, -1); got != *f.value {
			return nil, fmt.Errorf("sqlpolicy: limit on %s is %d, want %d", f.name, got, *f.value)
		}
	}
	pc := &Conn{conn: c, timeout: l.Timeout}
	if err := c.SetAuthorizer(pc.authorize); err != nil {
		return nil, fmt.Errorf("sqlpolicy: authorizer: %w", err)
	}
	return pc, nil
}

// Run compiles and runs each statement in sql in turn, interrupting it when
// the policy's timeout or ctx ends. For each statement it calls fn, which
// binds parameters and steps the statement; a nil fn steps it to completion.
// Run closes each statement after fn returns, and stops at the first error.
//
// A refusal by the policy, including the timeout and the memory limit, is
// returned as *Error; any other error comes from SQLite or from fn. After a
// panic in the engine, this Run and every later one return an error that
// wraps ErrBroken, or *Error with ReasonMemory for the first.
func (c *Conn) Run(ctx context.Context, sql string, fn func(*sqlite3.Stmt) error) (err error) {
	if c.broken {
		return ErrBroken
	}
	ctx, cancel := context.WithTimeoutCause(ctx, c.timeout, errTimeLimit)
	defer cancel()
	defer c.conn.SetInterrupt(c.conn.SetInterrupt(ctx))
	c.refused = ""

	rest := sql
	defer func() {
		// ncruces panics with an "out of memory" error when SQLite reports
		// SQLITE_NOMEM, which includes a statement over MaxVDBEOps, and with
		// a runtime error when the translated C code overflows its stack.
		// Either can leave the engine's state half-updated.
		r := recover()
		if r == nil {
			return
		}
		// shortcut: the out-of-memory value's type is internal to ncruces, so it is matched by message; recheck on upgrade.
		oom := fmt.Sprint(r) == "sqlite3: out of memory"
		if _, ok := r.(runtime.Error); !ok && !oom {
			panic(r)
		}
		c.broken = true
		if oom {
			err = &Error{Reason: ReasonMemory, SQL: prefix(rest)}
		} else {
			err = fmt.Errorf("%w: %v", ErrBroken, r)
		}
	}()
	for rest != "" {
		stmt, tail, err := c.conn.Prepare(rest)
		if err == nil && stmt != nil {
			err = run(stmt, fn)
		}
		if err != nil {
			return c.classify(ctx, rest, err)
		}
		if stmt == nil {
			break // only comments or white space remain
		}
		rest = tail
	}
	return nil
}

// Close closes the connection. After ErrBroken, SQLite may be unable to
// close it, and its files are then released only when Go finalizes them.
func (c *Conn) Close() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: close: %v", ErrBroken, r)
		}
	}()
	return c.conn.Close()
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

var errTimeLimit = errors.New("sqlpolicy: time limit")

func (c *Conn) classify(ctx context.Context, sql string, err error) error {
	switch {
	case c.refused != "":
		return &Error{Reason: c.refused, SQL: prefix(sql)}
	case errors.Is(err, sqlite3.INTERRUPT) && errors.Is(context.Cause(ctx), errTimeLimit):
		return &Error{Reason: ReasonTimeLimit, SQL: prefix(sql)}
	}
	return err
}

func (c *Conn) authorize(action sqlite3.AuthorizerActionCode, name3rd, name4th, _, _ string) sqlite3.AuthorizerReturnCode {
	r := decide(action, name3rd, name4th)
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
// table-valued function, and the ATTACH inside VACUUM).
func decide(action sqlite3.AuthorizerActionCode, name3rd, name4th string) Reason {
	switch action {
	case sqlite3.AUTH_SELECT, sqlite3.AUTH_READ, sqlite3.AUTH_RECURSIVE,
		sqlite3.AUTH_INSERT, sqlite3.AUTH_UPDATE, sqlite3.AUTH_DELETE,
		sqlite3.AUTH_TRANSACTION, sqlite3.AUTH_SAVEPOINT,
		sqlite3.AUTH_CREATE_TABLE, sqlite3.AUTH_CREATE_INDEX, sqlite3.AUTH_CREATE_VIEW, sqlite3.AUTH_CREATE_TRIGGER,
		sqlite3.AUTH_CREATE_TEMP_TABLE, sqlite3.AUTH_CREATE_TEMP_INDEX, sqlite3.AUTH_CREATE_TEMP_VIEW, sqlite3.AUTH_CREATE_TEMP_TRIGGER,
		sqlite3.AUTH_DROP_TABLE, sqlite3.AUTH_DROP_INDEX, sqlite3.AUTH_DROP_VIEW, sqlite3.AUTH_DROP_TRIGGER,
		sqlite3.AUTH_DROP_TEMP_TABLE, sqlite3.AUTH_DROP_TEMP_INDEX, sqlite3.AUTH_DROP_TEMP_VIEW, sqlite3.AUTH_DROP_TEMP_TRIGGER,
		sqlite3.AUTH_ALTER_TABLE, sqlite3.AUTH_REINDEX, sqlite3.AUTH_ANALYZE:
		return ""
	case sqlite3.AUTH_PRAGMA:
		if allowedPragmas[strings.ToLower(name3rd)] {
			return ""
		}
		return ReasonPragma
	case sqlite3.AUTH_FUNCTION:
		if name := strings.ToLower(name4th); allowedFunctions[name] || alterFunctions[name] {
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
// metadata and GORM's migrator issue, and quick_check, which SQLite runs
// itself in ALTER TABLE ADD COLUMN on a STRICT table or one with CHECK
// constraints. Every other pragma is refused.
var allowedPragmas = set(
	"table_info", "table_xinfo", "index_list", "index_info", "foreign_key_list", "quick_check",
)

// allowedFunctions are the SQL functions registered by ncruces v0.37.0 that
// compute only on their arguments. Every other function is refused, including
// sqlite_log, sqlite_compileoption_get, sqlite_compileoption_used,
// sqlite_source_id and match, and any function registered later.
var allowedFunctions = set(
	// Core scalar functions.
	"abs", "changes", "char", "coalesce", "concat", "concat_ws", "format", "glob", "hex", "if", "ifnull", "iif",
	"instr", "last_insert_rowid", "length", "like", "likelihood", "likely", "lower", "ltrim", "max", "min", "nullif",
	"octet_length", "printf", "quote", "random", "randomblob", "replace", "round", "rtrim", "sign", "soundex",
	"sqlite_version", "substr", "substring", "subtype", "total_changes", "trim", "typeof", "unhex", "unicode",
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
	"json_extract", "json_group_array", "json_group_object", "json_insert", "json_object", "json_patch",
	"json_pretty", "json_quote", "json_remove", "json_replace", "json_set", "json_type", "json_valid",
	"jsonb", "jsonb_array", "jsonb_array_insert", "jsonb_extract", "jsonb_group_array", "jsonb_group_object",
	"jsonb_insert", "jsonb_object", "jsonb_patch", "jsonb_remove", "jsonb_replace", "jsonb_set",
	// Extensions that ncruces registers on every connection.
	"base64", "decimal", "decimal_add", "decimal_cmp", "decimal_exp", "decimal_mul", "decimal_pow2", "decimal_sub",
	"decimal_sum", "ieee754", "ieee754_exponent", "ieee754_from_blob", "ieee754_from_int", "ieee754_inc",
	"ieee754_mantissa", "ieee754_to_blob", "ieee754_to_int", "regexp", "regexpi",
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
	ReasonAttach    Reason = "attach"     // ATTACH, and VACUUM, which attaches its output
	ReasonDetach    Reason = "detach"     // DETACH
	ReasonPragma    Reason = "pragma"     // a pragma outside the allowlist, in any form
	ReasonFunction  Reason = "function"   // an SQL function outside the allowlist
	ReasonStatement Reason = "statement"  // any other kind of statement, such as CREATE VIRTUAL TABLE
	ReasonTimeLimit Reason = "time_limit" // the statement ran past the policy's timeout
	ReasonMemory    Reason = "memory"     // the connection's memory, or MaxVDBEOps; the Conn is then broken
)

// Error is a refusal by the policy.
type Error struct {
	Reason Reason
	SQL    string // at most the first 64 bytes of the refused statement's text
}

func (e *Error) Error() string {
	return fmt.Sprintf("sqlpolicy: refused (%s): %q", e.Reason, e.SQL)
}

func prefix(sql string) string {
	const n = 64
	if len(sql) > n {
		sql = sql[:n]
	}
	return strings.ToValidUTF8(sql, "")
}
