---
title: "0007: SQL policy"
description: Client SQL runs one statement at a time under a default-deny SQLite authorizer, defensive configuration and limits measured to keep any combination of nesting inside the ncruces build's 64 KiB C stack, with a cooperative time limit, panic containment and a reset between principals, all applied by internal/sqlpolicy; the engine and the server carry the requirements the package cannot meet alone.
weight: 7
---

**Status:** Accepted

**Date:** 2026-11-15

## Context

skylite's requirements restrict the SQL that arrives from clients: no extension loading, no attaching arbitrary files, bounded query time and result size, and nothing a client sends may read or write outside its own database or change an engine setting the server depends on. The client protocol ([0005](/docs/decisions/0005-client-protocol/)) leaves this SQL policy to this record and fixes how it is consulted: before any step, on every path ([0005 decision 5](/docs/decisions/0005-client-protocol/#5-reads)). This record decides the part that sits next to the SQLite connection, and the pull request that adds it also adds `internal/sqlpolicy`, which applies it to a connection of the pure-Go SQLite binding [ncruces/go-sqlite3](https://github.com/ncruces/go-sqlite3/tree/v0.37.0) v0.37.0 (SQLite 3.54.1, MIT licence). The engine, the server and connection pooling come later; what they must do for this policy to hold is stated in [Requirements on the engine and the server](#requirements-on-the-engine-and-the-server).

**Threat model.** A client is authenticated but its SQL text is untrusted, and services of many tenants send SQL to the same server. Each connection serves one database, so the policy has to keep a statement inside that database and away from the server's own settings, its files and its process.

Facts the policy has to respect:

- [`sqlite3_stmt_readonly`](https://sqlite.org/c3ref/stmt_readonly.html) is true for `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `ATTACH` and `DETACH`, so "read-only" is not a safety check by itself.
- Some pragmas take effect when the statement is compiled, not when it runs, so the policy must decide before or during compilation. The [authorizer](https://sqlite.org/c3ref/set_authorizer.html) is consulted during compilation, which is where it decides. It sees each function by name only, not with its number of arguments.
- Because the authorizer runs at compile time, a compiled statement must never be reused under a different policy.

What the WebAssembly build means. ncruces compiles SQLite to WebAssembly and translates that into Go, so the engine is ordinary Go code with its own linear memory per connection:

- Extension loading does not exist. The build defines `SQLITE_OMIT_LOAD_EXTENSION` ([sqlite_opt.h#L38](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L38)), so neither `load_extension()` nor `sqlite3_load_extension` is compiled in.
- Each connection's memory is capped at 256 MiB by default ([wrap.go#L28](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/wrap.go#L28)); [`WithMaxMemory`](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/wrap.go#L18) can change it when the connection is opened. When SQLite reports `SQLITE_NOMEM`, ncruces panics instead of returning an error ([conn.go#L493-L494](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L493-L494)). SQLite reports `SQLITE_NOMEM` for a statement over the VDBE-operations limit as well ([vdbeaux.c#L187-L188](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbeaux.c#L187-L188)).
- The C stack is a fixed 64 KiB region at the start of linear memory ([build.sh#L23](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/build.sh#L23), [sqlite3.go#L33](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/sqlite3.go#L33)). A statement that recurses too deeply drives the stack pointer below zero; the translated code then panics with an index out of range, and every later call on that connection panics too. SQLite's default limits do not protect this build; [The C stack](#the-c-stack) gives the measurements.
- The configuration the build compiles in: foreign keys are enforced by default ([sqlite_opt.h#L22](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L22)) and `trusted_schema` is off by default ([sqlite_opt.h#L23](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L23)). SQLite's own default leaves foreign keys off ([foreignkeys.html, section 2](https://sqlite.org/foreignkeys.html#fk_enable)).
- `Conn.Config` rejects database-configuration options above `SQLITE_DBCONFIG_REVERSE_SCANORDER` ([config.go#L19-L20](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/config.go#L19-L20)), so `SQLITE_DBCONFIG_ENABLE_ATTACH_CREATE` and `_ATTACH_WRITE` cannot be set through it. The policy does not need them, because it refuses `ATTACH`.
- The authorizer callback receives a NULL argument as an empty string ([config.go#L221-L222](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/config.go#L221-L222)), so it cannot tell `PRAGMA foreign_keys` from `PRAGMA foreign_keys = ''`, which turns enforcement off. A pragma is therefore allowed or refused by name, whatever its argument.
- ncruces installs a progress handler on every connection that interrupts the running statement, every 1,000 virtual-machine instructions, once the context given to `SetInterrupt` is done ([conn.go#L120](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L120), [conn.go#L351-L361](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L351-L361)); *measured by the first review of this record,* it also fires while a statement is compiled. Work inside one instruction, such as one call of an SQL function or one page read, is never interrupted.

## Decision

### Where the policy applies

The engine opens each connection, does its own set-up (its VFS, `journal_mode`, `synchronous` and the rest that [0004](/docs/decisions/0004-storage-engine/) requires), then calls `Policy.Apply` once. From then on:

- The server's own statements, `BEGIN`, `COMMIT`, `ROLLBACK`, its savepoints and its pragmas, go through `Conn.Privileged`, which runs a function with the authorizer suspended and restores it when the function returns or panics. The server qualifies its own SQL with `main.`, because a client's temporary objects can shadow names in `main`.
- Every client statement goes through `Conn.Run`, one statement per call. Before compiling, `Run` refuses SQL longer than `MaxSQLLength` (reason `sql_length`) and SQL holding a NUL byte (`nul`), whose text after the NUL SQLite would otherwise ignore. After compiling the first statement and before any step, it refuses text after it other than white space, comments and the statement's own semicolon, and SQL that holds no statement (`statement_count`), as [0005 decision 6](/docs/decisions/0005-client-protocol/#6-statements-transactions-and-errors) requires. The server answers these three with `invalid_request`, a refusal by the authorizer with `statement_forbidden`, `time_limit` with `timeout`, and `memory` or a broken connection with `internal`.

`Run` compiles the statement, runs it and closes it, so no compiled statement outlives the request it was compiled for, and there is no statement cache. `sqlite3_set_authorizer` also expires every statement already prepared on the connection ([auth.c#L81](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/auth.c#L81)), so a statement prepared before `Apply` or inside `Privileged` is compiled again, under the policy, if it is stepped later.

### Mechanisms

| Mechanism | What it is for |
|---|---|
| Authorizer, default-deny ([docs](https://sqlite.org/c3ref/set_authorizer.html)) | Decides, while each statement is compiled, which actions, pragmas and functions it may use. Any action code the policy does not list is refused, including ones a later SQLite adds. |
| One statement per `Run`, no NUL, bounded text | The protocol's statement rules, enforced before any step. |
| `SQLITE_DBCONFIG_DEFENSIVE` on ([docs](https://sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigdefensive)) | Disables the language features that can corrupt the file, such as `PRAGMA writable_schema = ON` and `PRAGMA journal_mode = OFF`, as a second line behind the pragma allowlist. |
| `SQLITE_DBCONFIG_WRITABLE_SCHEMA` off | Clears a writable schema set before `Apply`. |
| `trusted_schema` off ([docs](https://sqlite.org/pragma.html#pragma_trusted_schema)) | Views, triggers, `CHECK` constraints, defaults and generated columns may call only functions marked innocuous, so a schema object cannot run a function the client could not call directly. |
| `SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION` off ([docs](https://sqlite.org/c3ref/enable_load_extension.html)) | Kept off although the build omits extension loading, so the setting is right if a later build includes it. |
| `temp_store = MEMORY` | Temporary tables and indices, statement journals and sorts stay in the connection's memory; see [Temporary tables](#temporary-tables). |
| Run-time limits ([docs](https://sqlite.org/c3ref/limit.html)) | Bound the size of one statement and one value, and keep every nesting that recurses in C inside the 64 KiB stack; see [Limits](#limits). The number of attached databases is fixed at zero. |
| Page count, optional | `Limits.MaxPageCount` sets `max_page_count` through `Privileged`, so the server can bound a database's size. Unset by default. |
| Two-argument `trim`, `ltrim`, `rtrim` | Replaced by the policy's own, with a bounded character set; see [Functions](#functions). |
| Time limit | `Run` gives the connection a context that ends at the policy's timeout, and the progress handler interrupts the statement then ([docs](https://sqlite.org/c3ref/interrupt.html)). It is cooperative; see [Time](#time). |
| Panic containment | `Run`, `Privileged` and `Reset` recover a panic raised inside ncruces, for `SQLITE_NOMEM` or a stack overflow, and mark the connection broken. A panic raised in the caller's own code is passed on. |
| `Conn.Reset` | Returns everything a client can change on the connection to the policy's defaults; see [Session state](#session-state). |

Authorizer actions allowed: `SELECT`, reading a column, recursive common table expressions, `INSERT`, `UPDATE`, `DELETE`, creating and dropping tables, indices, views and triggers (temporary or not), `ALTER TABLE`, `REINDEX`, `ANALYZE`, savepoints inside a transaction, and the pragmas and functions listed below. Refused: transaction control, `ATTACH`, `DETACH`, `CREATE VIRTUAL TABLE`, `DROP VIRTUAL TABLE`, and any other action code. Writes to `sqlite_schema` cannot be refused by the authorizer, because SQLite authorizes its own write to `sqlite_schema` with the same code when it creates a table ([build.c#L1320](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/build.c#L1320)); defensive mode and `writable_schema` off refuse them instead.

**Transaction control.** Transactions are the protocol's operations, not SQL ([0005 decision 5](/docs/decisions/0005-client-protocol/#5-reads)). The policy refuses `BEGIN`, `COMMIT`, `END` and `ROLLBACK` in every client statement, with reason `transaction`. `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` are allowed only when a transaction is open as `Run` starts, which on the protocol's paths means a transaction the server opened on a channel; outside one, `SAVEPOINT` would begin a transaction, and the policy refuses all three. The server opens and ends transactions through `Privileged`.

**Schema in the file.** SQLite does not consult the authorizer for schema text it loads from the database file. A view, trigger, `CHECK` constraint or default written through a path without the policy, for example with a function the policy refuses, runs when a client's statement uses it. The schema of a skylite database must therefore only ever be written through the policy.

### Limits

Each limit is a field of `sqlpolicy.Limits`. The defaults below are the package's; the server sets every field from its own configuration, and each sits under the protocol limit named in the last column ([0005 decision 9](/docs/decisions/0005-client-protocol/#9-limits-and-defaults)). `New` refuses values outside SQLite's compiled range and combinations of the nesting limits above the measured stack budget, and `Apply` reads each limit back.

| Limit | Default | SQLite's default here | Why | Under |
|---|---|---|---|---|
| `SQLITE_LIMIT_LENGTH` | 1 MiB | 1,000,000,000 | One string or BLOB. Bounds the work of a function the time limit cannot interrupt; see [Time](#time). | Request body (16 MiB), held result (1 MiB), result size (64 MiB) |
| `SQLITE_LIMIT_SQL_LENGTH` | 1 MiB | 1,000,000,000 | One statement's text; longer SQL is refused with `sql_length` before compiling. | Request body |
| `SQLITE_LIMIT_EXPR_DEPTH` | 18 | 1,000 | Depth of an expression, and nesting of views, CTEs and subqueries; see [The C stack](#the-c-stack). | — |
| `SQLITE_LIMIT_PARSER_DEPTH` | 100 | 2,500 | Nesting in parentheses; 100 allows 15 nested subqueries in `FROM`. The parser's first 50 entries are static, so values below 50 act as 50 ([parse.y#L25](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/parse.y#L25), [lempar.c#L306-L311](https://github.com/sqlite/sqlite/blob/version-3.54.1/tool/lempar.c#L306-L311)), and `New` refuses them. | — |
| `SQLITE_LIMIT_COMPOUND_SELECT` | 4 | 500 | Terms of one compound `SELECT`; see [The C stack](#the-c-stack). | — |
| `SQLITE_LIMIT_TRIGGER_DEPTH` | 8 | 1,000 | Nesting of triggers and foreign key actions, at compile time and at run time; see [The C stack](#the-c-stack). | — |
| `SQLITE_LIMIT_LIKE_PATTERN_LENGTH` | 4,096; at most 4,096 | 50,000 | Matching recurses once per wildcard and overflowed the stack from about 9,000 bytes; its time grows with pattern length times text length ([Time](#time)). | — |
| `SQLITE_LIMIT_VDBE_OP` | 250,000,000 | 250,000,000 | Not lowered: the time limit, which also acts during compilation, bounds time, and the memory cap bounds memory. | — |
| `SQLITE_LIMIT_FUNCTION_ARG` | 1,000 | 1,000 | Kept; nothing found that it endangers. | — |
| `SQLITE_LIMIT_VARIABLE_NUMBER` | 32,766 | 32,766 | Kept; ORMs bind one parameter per element of an `IN` list. | Request body |
| `SQLITE_LIMIT_ATTACHED` | 0 | 10 | Fixed, not configurable; a second refusal of `ATTACH` and `VACUUM`. | — |
| `max_page_count` | unset | 4,294,967,294 | Optional bound on a database's size; `Apply` fails if the database is already larger. | — |
| Time limit | 5 s | none | Per `Run`; the server sets it to what remains of the request's statement time. | Statement time (30 s) |

A limit that SQLite enforces fails the statement with SQLite's own error, which names the limit; the policy passes it through as an engine error and does not parse messages.

### The C stack

*Measurements, on this build:* SQLite 3.54.1 in ncruces v0.37.0 and go-sqlite3-wasm v6.5.35401, Go 1.26.11, linux/amd64, a new in-memory connection for each statement, SQLite's own limits so that only the stack could stop a statement. The figures are what broke the connection, not properties SQLite documents, and they must be measured again on any upgrade of the binding.

- Alone: a chain of 332 operators (`SELECT 1 + 1 + …`); 220 `UNION ALL` terms; 263 nested subqueries in `FROM`; a chain of 197 views; a chain of 70 `INSERT` triggers (137 `UPDATE`, 147 `DELETE`, 132 `ON DELETE CASCADE`); a `LIKE` pattern of about 9,000 bytes alternating `%` and a character.
- Combined, the limits multiply. With *T* terms in a compound `SELECT` at each of *N* levels, the nested part in the first term, the connection broke when *N* × *T* reached about 180 to 220: at 19 levels of 10-term `INTERSECT` or `EXCEPT` compounds, 22 of `UNION ALL`, 44 levels of 5 terms and 110 of 2. The figure was the same for subqueries in `FROM`, chains of views and chains of CTEs, and a mix of the three broke no earlier. An expression of depth 50 at the innermost level moved the break by at most one level. Scalar and `IN` subqueries never broke: SQLite's expression-height check stopped them first.
- Trigger nesting adds to the same stack: with a nested statement in the deepest trigger of an `INSERT` chain, *N* × *T* at the break fell by about 2 to 2.5 per level of the chain (to 168 at 8 levels, 148 at 16, 100 at 32), and by less for `UPDATE`, `DELETE` and cascade chains.

Which limit bounds which nesting: views, CTEs and subqueries in `FROM` count against `SQLITE_LIMIT_EXPR_DEPTH` ([select.c#L2497-L2500](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/select.c#L2497-L2500)), which allowed *E* − 2 levels of views or CTEs; subqueries in `FROM` are also bounded by the parser depth, which allowed 6 levels at 50, 15 at 100 and 31 at 200; triggers and foreign key actions by `SQLITE_LIMIT_TRIGGER_DEPTH`, which SQLite checks while it compiles a chain ([trigger.c#L1259-L1261](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/trigger.c#L1259-L1261)) and again when it runs one ([vdbe.c#L7683-L7686](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbe.c#L7683-L7686)).

The rule. In the unit of one compound term at one level, a statement within the limits costs at most *T* × max(*E* − 2, *P* / 6) + 2.5 × *D*, for compound terms *T*, expression depth *E*, parser depth *P* and trigger depth *D*. `New` refuses any configuration whose cost exceeds 90, half the 180 at which the connection broke, so an operator cannot configure the overflow back in. The defaults cost 84. *Measured margins at the defaults:* chains of views or CTEs reach 16 levels and broke at 47 (4-term `INTERSECT`), 2.9 times; subqueries in `FROM` reach 15 levels and broke at 48, 3.2 times; a trigger chain of 8 with the same nesting in its last trigger broke at 42; an expression reaches depth 17 against 332; a `LIKE` pattern 4,096 bytes against about 9,000. `TestNestingAtTheLimitsStaysInsideTheStack` runs every one of these shapes at the deepest the limits allow, at the defaults and at three corners of what `New` accepts (11 compound terms; expression depth 90 with one term; trigger depth 33), and checks that each fails with SQLite's error and leaves the connection usable.

What the defaults cost clients: an expression deeper than 17, such as a chain of 17 `AND` terms; a compound `SELECT` of more than 4 terms; views or CTEs nested more than 16 deep; and a chain of triggers or foreign key actions deeper than 8, which includes deleting a self-referencing `ON DELETE CASCADE` tree more than 8 levels deep (*measured:* depth 8 succeeds, depth 9 fails with "triggers nested too deep"). The server may trade one for another within the budget.

### Time

The time limit is cooperative. The progress handler stops a statement between virtual-machine instructions and during compilation, but not inside one instruction. *Measurements, on this build, by the first review of this record,* with a 300 ms limit, none interrupted: `regexp` on 10,000 characters with `a{1000}b` took 4.4 s, about 0.5 ms per character, and `((a{10}){10}){10}b` on 1 MiB ran more than 8 minutes before it was killed; `decimal_pow2(20000)` took 5.2 s; `decimal_mul` took 1.7 s at 20,000 digits and 6.1 s at 40,000; on a 1.1 GiB table, `DELETE FROM big` took 868 ms and succeeded, and `PRAGMA quick_check` 947 ms.

So the policy refuses or replaces every function whose time is not close to linear in its input, and bounds the rest through `MaxLength`. *Sweep, on this build:* every function the build registers, called with every assignment of up to three arguments from a set of inputs, timed at two sizes. A first pass at 64 KiB and 128 KiB used nine inputs: a run of `a`; a run of `a` of half the length ending in `b`; the same with the letters swapped; runs of `g` and of `b` ending in `g`; a JSON object of distinct keys; a JSON array; a long JSON path; nested JSON arrays. The full pass at 512 KiB and 1 MiB used seven, without the two `g` inputs and the nested arrays and with a set of 64 characters, after the changes below: 120 functions and 9,843 calls, six processes at once. The calls whose time grew about four times when the input doubled:

| Function | Time at 128 KiB | Decision |
|---|---|---|
| `trim`, `ltrim`, `rtrim` with two arguments, a set of 64 Ki characters matching only at its end | 35 to 91 s | Replaced: the policy's own, the same comparisons as SQLite's `trimFunc` ([func.c#L1578-L1657](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/func.c#L1578-L1657)), refused with reason `function` when the set holds more than 64 characters. Linear: 0.67 s at 1 MiB with 64 characters. |
| `unhex` with two arguments, a long set of characters to skip | 15 s | Refused; neither ORM uses it. The authorizer cannot refuse only the two-argument form. |
| `json_patch`, `jsonb_patch` | 1.0 to 1.6 s | Refused; neither ORM uses them. |
| `replace`, `instr` with a needle of half the text | 0.11 to 0.15 s | Kept, bounded by `MaxLength`; see the table below. |

Of every other call, those that grew more than 2.8 times or took over 20 ms at 1 MiB in the full pass, 187 calls, were run again one at a time, and grew at most 2.6 times. `regexp`, `regexpi` and the `decimal` functions are refused on the review's measurements above and were not swept.

*Worst uninterruptible times, on this build,* one statement each, on 8 cores: at 1 MiB one at a time on an otherwise idle machine, with a time limit of 100 ms; at 4 MiB and 16 MiB with a limit of 2 s, so that binding the arguments fitted in it, while other work loaded the machine (load average up to about 40), so those two columns overstate what the statement costs alone: at 4 MiB by up to about 40% against the 1 MiB figures scaled by the size, or by its square for `replace` and `instr`. The time limit had passed in every case before the statement ended:

| Statement | 1 MiB | 4 MiB | 16 MiB |
|---|---|---|---|
| `replace(x, y, 'c')`, *x* a run of `a`, *y* half of it ending in `b` | 7.5 s | 2 min 44 s | 1 h 16 min |
| `instr(x, y)`, the same | 8.1 s | 2 min 44 s | 1 h 15 min |
| `x LIKE '%' \|\| 'a'×4094 \|\| 'b'` | 15.4 s | 1 min 28 s | 7 min 29 s |
| `x GLOB '*' \|\| 'a'×4094 \|\| 'b'` | 15.8 s | 1 min 37 s | 7 min 4 s |
| `x GLOB '*' \|\| '[a]'×1364 \|\| 'b'` | 22.9 s | 1 min 59 s | 9 min 7 s |
| `trim(x, y)`, *y* 64 characters, *x*'s only at the end | 0.67 s | 2.8 s | 13 s |

At the default `MaxLength` of 1 MiB one statement can therefore hold its connection for about 23 s past the time limit; `replace` and `instr` grow with the square of `MaxLength`, and `LIKE` and `GLOB` with `MaxLength` times `MaxLikePatternLength`. The server may raise `MaxLength`, accepting the times in the table, or lower `MaxLikePatternLength`.

`PRAGMA quick_check` is allowed only in a `Run` whose statement the authorizer has already seen as `ALTER TABLE`: SQLite runs it itself, through `pragma_quick_check`, when `ALTER TABLE … ADD COLUMN` changes a STRICT table or one with `CHECK` constraints ([alter.c#L472-L487](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/alter.c#L472-L487)). Its time grows with the table, like `DELETE FROM` a large table or building an index, which no limit here bounds; requirement 3 below does.

When the time limit interrupts a write inside a transaction, SQLite rolls back the whole transaction; an interrupted read leaves the transaction open. Both are tested. If the caller's own context ends first, `Run` returns the engine's `SQLITE_INTERRUPT` error rather than a policy refusal.

### Memory and a broken connection

The connection's memory cap is the backstop for memory: a statement that exhausts it is refused with reason `memory` and breaks the connection. Deep view expansion ends that way: *measured by the first review,* about 700 bytes of SQL, 30 chained views with small compounds in a `WHERE … IN`, reached the 256 MiB cap in about 0.5 s with a VDBE-operations limit of 1, 4 or 250 million alike. The requirements below make a broken connection safe.

After `Run` recovers a panic from the engine, it returns `*Error` with reason `memory` for `SQLITE_NOMEM` or an error wrapping `ErrBroken` for anything else, and every later `Run`, `Privileged` and `Reset` on that connection returns `ErrBroken`. `Conn.Close` tries to close it; after a stack overflow SQLite cannot.

A broken connection can keep its locks. *Measured by the first review, on this build:* on a database file in WAL mode, connection A ran `BEGIN IMMEDIATE` and an `INSERT`, then a statement that overflowed the stack; `Run` and `Close` returned `ErrBroken`. A second connection's `INSERT` then failed with "database is locked", and still did after 47 s and about 400 forced garbage collections. Go's finalizers had closed A's file descriptors, but `/proc/locks` still showed A's OFD locks on the `-shm` file (a write lock at byte 120, read locks at 123 and 128): SQLite never reached `xShmUnmap` ([shm_ofd.go#L158-L176](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/vfs/shm_ofd.go#L158-L176)), and the file's mapping keeps the open file description, and its locks, alive. An overflow in autocommit or in a read transaction did not block a writer. The package cannot tell whether a write transaction was open when the engine broke, because the failing statement may have begun one and the engine's state is then unknown; the engine must assume that one was.

### Session state

A client can change its connection in three ways: `PRAGMA foreign_keys`, objects in the temp schema, and the last inserted row id. `Conn.Reset` rolls back an open transaction, drops every table, view and trigger in the temp schema, and their indices with them, with foreign key enforcement off so that a temporary child table cannot block the drop of its parent, turns enforcement back on and sets the last inserted row id to zero, then checks the result; if anything fails the connection is broken. The temp schema's `sqlite_sequence` table, which SQLite does not let be dropped, is left empty. `changes()` and `total_changes()` still count the earlier principal's rows; they reveal counts, not data, within the same database.

**`PRAGMA foreign_keys`** is a session setting that clients may read and change. GORM's table rebuild turns it off and on again around a transaction (below), and refusing it silently deleted referencing rows. Because the authorizer cannot tell a read of a pragma from a write, it is allowed by name.

### Pragmas

Every pragma is refused unless it is on this list. The policy decides by the pragma's name, in any letter case, for `PRAGMA x`, `PRAGMA x = y`, `PRAGMA x(y)` and schema-qualified forms. A pragma's table-valued function, such as `pragma_journal_mode`, compiles the pragma when the statement scans the function ([pragma.c#L3011-L3021](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pragma.c#L3011-L3021)), and the authorizer refuses it then; a statement whose plan never scans it succeeds without running the pragma.

| Allowed pragma | Why |
|---|---|
| `table_info` | Read-only column list. JDBC `getPrimaryKeys` in the reference driver, sqlite-jdbc ([JDBC3DatabaseMetaData.java#L2020](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L2020)). |
| `table_xinfo` | Read-only column list including hidden columns. sqlite-jdbc `getColumns` ([#L980](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L980)). |
| `index_list` | Read-only. sqlite-jdbc `getIndexInfo` ([#L1543](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L1543)); GORM `GetIndexes` as `PRAGMA_index_list(?)` ([migrator.go#L342](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L342)). |
| `index_info` | Read-only. sqlite-jdbc ([#L1567](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L1567)); GORM ([migrator.go#L350](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L350)). |
| `foreign_key_list` | Read-only. sqlite-jdbc `getImportedKeys` ([#L1430](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L1430)). |
| `foreign_keys` | A session setting; see [Session state](#session-state). GORM ([migrator.go#L18-L27](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L18-L27)). |
| `quick_check`, inside `ALTER TABLE` only | See [Time](#time). |

Settings the server owns, which clients may not read or change through a pragma: `journal_mode`, `synchronous`, `locking_mode`, `page_size`, `wal_checkpoint`, `wal_autocheckpoint`, `journal_size_limit`, `fullfsync`, `checkpoint_fullfsync`, `auto_vacuum`, `incremental_vacuum`, `encoding`, `secure_delete`, `max_page_count`, `schema_version`, `user_version`, `application_id`, `writable_schema`, `trusted_schema`, `cell_size_check`, `ignore_check_constraints`, `legacy_alter_table`, `defer_foreign_keys`, `recursive_triggers`, `query_only`, `read_uncommitted`, `temp_store`, `temp_store_directory`, `cache_size`, `cache_spill`, `mmap_size`, `threads`, `soft_heap_limit`, `hard_heap_limit`, `busy_timeout`, `analysis_limit` and every other pragma of the build. Refused introspection includes `database_list`, which shows the database file's path on the server, and `integrity_check`.

### ATTACH, DETACH and VACUUM

`ATTACH` and `DETACH` are refused by the authorizer in every form: a plain or URI filename, `:memory:`, an empty name, or a filename computed by an expression. SQLite does not accept `ATTACH` inside a trigger body or a view, so those are syntax errors. `VACUUM` consults no authorizer action of its own ([vacuum.c#L105-L138](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vacuum.c#L105-L138)); while it runs, it attaches its output with `ATTACH` ([vacuum.c#L226](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vacuum.c#L226)), the authorizer refuses that, and the attached-database limit of zero would refuse it again ([attach.c#L170](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/attach.c#L170)). So `VACUUM`, `VACUUM main` and `VACUUM INTO` are refused, with reason `attach`; compacting the file belongs to the engine. `VACUUM temp` is accepted and does nothing, because SQLite generates no vacuum for the temp schema ([vacuum.c#L126-L132](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vacuum.c#L126-L132)).

### Temporary tables

Clients may create and drop temporary tables, indices, views and triggers, including temporary triggers on their own main tables: they are private to the connection and hold only the client's own data. (Hibernate's SQLite dialect does not use them; it keeps the default of ordinary tables for its bulk-operation tables, [Dialect.java#L4018-L4020](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/dialect/Dialect.java#L4018-L4020).) With `temp_store = MEMORY`, SQLite keeps temporary databases in memory ([main.c#L2725-L2741](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/main.c#L2725-L2741)) and opens statement journals in memory ([btree.c#L3747](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/btree.c#L3747), [pager.c#L4581-L4584](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pager.c#L4581-L4584)); *measured,* sorts that spill SQLite's page cache opened no file either. Otherwise ncruces creates unnamed files in the process's temporary directory ([os_linux.go#L14-L41](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/vfs/os_linux.go#L14-L41)). Temporary objects count against the connection's memory, and `Reset` drops them, so they live no longer than the client's hold on a connection. A temporary object can shadow a `main` object of the same name, which is why the server qualifies its own SQL with `main.`.

### Functions

Every SQL function is refused unless it is on the allowlist in `internal/sqlpolicy`: the core scalar, aggregate, window, date and time, math and JSON functions, `base64` and the `ieee754` functions. A test pins the allowlist and the refused list, and fails if ncruces v0.37.0 registers a function, according to `pragma_function_list`, that is on neither. Refused: `sqlite_log`, which writes to the server's log; `sqlite_compileoption_get`, `sqlite_compileoption_used` and `sqlite_source_id`, which describe the server's build; `match`, which only full-text search uses and this build does not include; and, for their time ([Time](#time)), `regexp`, `regexpi`, `unhex`, `json_patch`, `jsonb_patch` and the `decimal` functions. Functions that read files or load code (`load_extension`, `readfile`, `writefile`, `edit`, `fts3_tokenizer`) are not in this build, and are refused if a later build or the server registers one. `sqlite_version` stays, because JDBC `DatabaseMetaData` reports the engine's version.

`Apply` registers the policy's own two-argument `trim`, `ltrim` and `rtrim`, which take the place of SQLite's ([create_function.html](https://sqlite.org/c3ref/create_function.html): "Built-in functions may be overloaded by new application-defined functions"; SQLite's registrations at [func.c#L3409-L3414](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/func.c#L3409-L3414)). They keep SQLite's results, which a test compares on text, BLOB, numeric, NULL and malformed UTF-8 arguments, and refuse a set of more than 64 characters. Hibernate's SQLite dialect uses the two-argument forms for a `trim` of anything other than white space ([SQLiteDialect.java#L339-L352](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L339-L352)); the one-argument forms stay SQLite's.

SQLite's internal functions for `ALTER TABLE` are allowed: SQLite calls them from SQL it compiles itself, the authorizer sees them first ([resolve.c#L1204](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/resolve.c#L1204)), and SQLite resolves them only in that SQL ([resolve.c#L1251-L1263](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/resolve.c#L1251-L1263)), so a client calling one gets "no such function". All nine are pinned, and a test runs every form of `ALTER TABLE` that SQLite 3.54.1 parses ([parse.y#L1892-L1933](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/parse.y#L1892-L1933)): `RENAME TO`, `RENAME [COLUMN]`, `ADD [COLUMN]` (including the forms that run `quick_check`), `DROP [COLUMN]`, `ADD CONSTRAINT … CHECK`, `ADD CHECK`, `DROP CONSTRAINT`, and `ALTER [COLUMN] … SET NOT NULL` and `DROP NOT NULL`. Refusing one of them inside SQLite's own `ALTER` SQL panicked the connection in the first review.

With `trusted_schema` off, a function that is not marked innocuous, such as `base64`, works in a query but not in a view, trigger or `CHECK` constraint.

### Errors

A refusal is an `*sqlpolicy.Error` with a stable reason code and no SQL text, which can hold secrets. Everything else is the engine's error, unchanged.

| Reason | Rule |
|---|---|
| `attach` | `ATTACH`, and `VACUUM`, which attaches its output |
| `detach` | `DETACH` |
| `transaction` | `BEGIN`, `COMMIT`, `END`, `ROLLBACK`; `SAVEPOINT`, `RELEASE` and `ROLLBACK TO` outside a transaction |
| `pragma` | A pragma not on the allowlist, in any form |
| `function` | A function not on the allowlist, or a two-argument trim with more than 64 characters to trim |
| `statement` | Any other action the authorizer refuses, such as `CREATE VIRTUAL TABLE` |
| `statement_count` | No statement, or text after the first one |
| `nul` | A NUL byte in the SQL |
| `sql_length` | SQL longer than `MaxSQLLength` |
| `time_limit` | The statement ran past the time limit |
| `memory` | The connection's memory; the connection is then broken |

### What the two ORMs need

Hibernate ORM 7.4.13 with the community SQLite dialect issues no pragmas ([SQLiteDialect.java](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java)). For the JPA acceptance suite in `clients/java/acceptance`, its schema creation emitted `drop table if exists` and `create table` with inline `unique`, then `select last_insert_rowid()` after each insert of a generated identifier. Its schema validation reads JDBC metadata; skylite's driver does not exist yet, so the test uses the queries the reference driver, sqlite-jdbc 3.53.4.0, runs for those calls. sqlite-jdbc builds `getColumns` as one compound `SELECT` with one term per column of every table that matches ([JDBC3DatabaseMetaData.java#L998](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L998)); under this policy that fails past 4 columns. skylite's JDBC driver must not build metadata results that way.

GORM v1.31.2 with `gorm.io/driver/sqlite` v1.6.0 runs, for `AutoMigrate`, queries against `sqlite_master` ([migrator.go#L29-L76](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L29-L76), [#L109-L155](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L109-L155), [#L208-L225](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L208-L225), [#L281-L298](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L281-L298)), `CREATE TABLE`, `CREATE INDEX` ([migrator.go#L252-L279](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L252-L279)) and `ALTER TABLE ? ADD ? ?` ([gorm migrator.go#L122-L203](https://github.com/go-gorm/gorm/blob/v1.31.2/migrator/migrator.go#L122-L203), [#L386-L387](https://github.com/go-gorm/gorm/blob/v1.31.2/migrator/migrator.go#L386-L387)); `GetIndexes` uses `PRAGMA_index_list` and `PRAGMA_index_info`. `CurrentDatabase` reads `PRAGMA database_list` ([migrator.go#L227-L231](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L227-L231)), which is refused; the SQLite migrator does not call it for `AutoMigrate`.

GORM changes a column, a constraint or drops a column by rebuilding the table: in a transaction, it creates `<table>__temp`, copies the rows, drops the table and renames the copy ([migrator.go#L376-L430](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L376-L430)). Dropping a table that other tables reference with enforcement on first deletes its rows, which fires `ON DELETE CASCADE` and `SET NULL` actions or, for other actions, fails the `DROP` ([foreignkeys.html, section 5](https://sqlite.org/foreignkeys.html)). Only `DropTable` and `AlterColumn` turn enforcement off around the work, with `RunWithoutForeignKey`, which reads `PRAGMA foreign_keys` and sets it off and on again outside the transaction, ignoring errors ([migrator.go#L18-L27](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L18-L27), [#L37-L38](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L37-L38), [#L78-L80](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L78-L80)). Under this policy, which allows the pragma, those two keep the referencing rows, as a test shows. `DropColumn`, `CreateConstraint` and `DropConstraint` rebuild without it ([#L157-L158](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L157-L158), [#L168-L172](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L168-L172), [#L193-L200](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L193-L200)): on a table others reference, they delete or null the referencing rows, or fail, under skylite and on any SQLite connection where the application has turned enforcement on. On SQLite's default, enforcement off, nothing is lost. The setting holds on the connection that ran it, so the pragma and GORM's transaction must reach the same server connection for the rows to survive; the Go driver's design has to ensure that.

The test suite runs each of these statements on a database in memory and in a file, and all pass.

### Testing

The package's tests pin every default, the allowed and refused functions and pragmas, and the internal `ALTER` functions as literals. `FuzzRun` runs arbitrary SQL on a database file that holds the server's settings (WAL, `synchronous = FULL`, no automatic checkpoint, and values other than SQLite's defaults for the rest) and checks after each input that every setting the server owns, every database configuration flag and limit, the attached databases, the database file's size and `integrity_check` are unchanged, and that SQLite opened no file but the database's own. Run it with `go test ./internal/sqlpolicy -run '^$' -fuzz FuzzRun -fuzztime 3m -fuzzminimizetime 0`; without `-fuzzminimizetime 0`, Go's minimizer spends up to a minute on each new input of a target that costs milliseconds, and in the first review half the intervals reported no executions.

## Requirements on the engine and the server

The package cannot meet these alone. Making a stack overflow unreachable within the limits is the main defence; these hold when something breaks anyway.

1. **Locks live in the engine's VFS.** Lock and shared-memory state for skylite's databases lives in the engine's own VFS. When a connection is reported broken (`ErrBroken`, or a `memory` refusal), the engine releases everything that connection held through the VFS and discards its uncommitted transaction. If the broken connection had reached the commit path, the fail-stop rule of [0004 decision 6](/docs/decisions/0004-storage-engine/#6-commit-outcomes-and-fail-stop) applies. Because the package cannot tell whether a write transaction was open, the engine assumes it was.
2. **A broken connection is never reused,** and is replaced.
3. **The deadline reaches page reads.** The engine's VFS checks the statement's deadline on every page read and fails the read once it has passed, so that a single operation that touches many pages, such as deleting every row of a large table or an integrity check, ends with an error.
4. **Reset between holders.** The server calls `Conn.Reset` before a connection is used by a different channel or principal, and discards a connection for which it fails. That includes the pooled connections of the stateless read path: `PRAGMA foreign_keys = OFF` passes SQLite's read-only test (*measured,* `Stmt.ReadOnly` is true for it), so it reaches that path as a pure read, and [0005 decision 6](/docs/decisions/0005-client-protocol/#6-statements-transactions-and-errors) shares those connections on the ground that a pure read leaves no state behind. The server resets a pooled connection before every reuse, or refuses the pragma on that path.
5. **Order of set-up.** The engine calls `Apply` after its own set-up of a connection and issues every later statement of its own through `Privileged`, qualified with `main.`.
6. **Schema only through the policy.** Nothing writes a skylite database's schema except client statements under this policy and the server's own statements.

### Not in the first version

- A policy per principal, for example read-only clients, and row-level rules.
- Resource accounting per tenant. The per-connection memory cap is the engine's choice when it opens a connection.
- Result size and row count, which belong to the server.

## Alternatives considered

- **Parse SQL with a separate parser and decide on its syntax tree.** Rejected: the policy would have to track SQLite's grammar exactly, and any statement it parsed differently from SQLite would bypass it. The authorizer sees what SQLite actually compiles, including statements SQLite compiles itself, views, triggers and the `ATTACH` inside `VACUUM`.
- **A denylist of dangerous actions, pragmas and functions.** Rejected: SQLite and ncruces add pragmas, functions and authorizer codes between releases, and a denylist would allow each new one until someone noticed. A default-deny allowlist refuses them, and the registry test reports them.
- **`sqlite3_stmt_readonly` or `PRAGMA query_only` as the guard.** Rejected: neither refuses `ATTACH`, pragmas or functions, and `sqlite3_stmt_readonly` is true for `ATTACH`.
- **Bounding each nesting limit alone.** Rejected: the first review broke the connection at the earlier defaults with three levels of 99-term compounds, 5,642 bytes of SQL. Only the product is bounded.
- **Refusing `trim`, `ltrim` and `rtrim`.** Rejected: Hibernate uses them, and the authorizer cannot refuse only the two-argument forms.
- **Server-owned `foreign_keys`.** Rejected: GORM's table rebuild ignores the refusal and deletes referencing rows.

## Consequences

- The engine must apply the policy to every connection that runs client SQL, run that SQL only through `Conn.Run` and its own through `Conn.Privileged`, and meet the requirements above.
- skylite's JDBC driver must answer `DatabaseMetaData` without a compound `SELECT` per column.
- Clients meet the nesting limits of [The C stack](#the-c-stack), including cascade trees of at most 8 levels at the defaults.
- One statement can hold its connection for tens of seconds past the time limit at the default `MaxLength`; see [Time](#time).
- GORM's `DropColumn`, `CreateConstraint` and `DropConstraint` on tables that other tables reference delete or null the referencing rows, as on any SQLite connection with enforcement on.
- Upgrading ncruces means rerunning the package's tests, which check the function and pragma registries and the stack shapes at the limits, and measuring the stack and the function sweep again.
