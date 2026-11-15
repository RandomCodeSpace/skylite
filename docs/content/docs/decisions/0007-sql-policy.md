---
title: "0007: SQL policy"
description: Client SQL runs under a default-deny SQLite authorizer, defensive configuration and limits set low enough for the ncruces build's 64 KiB C stack, with a time limit and panic containment, all applied by internal/sqlpolicy.
weight: 7
---

**Status:** Accepted

**Date:** 2026-11-15

## Context

skylite's requirements restrict the SQL that arrives from clients: no extension loading, no attaching arbitrary files, bounded query time and result size, and nothing a client sends may read or write outside its own database or change an engine setting the server depends on. No server or storage engine exists yet. This record decides the part of that requirement that sits next to the SQLite connection, and the pull request that adds it also adds `internal/sqlpolicy`, which applies it to a connection of the pure-Go SQLite binding [ncruces/go-sqlite3](https://github.com/ncruces/go-sqlite3/tree/v0.37.0) v0.37.0 (SQLite 3.54.1, MIT licence). The engine, the HTTPS layer and connection pooling come later and are not part of this decision.

**Threat model.** A client is authenticated but its SQL text is untrusted, and services of many tenants send SQL to the same server. Each connection serves one database, so the policy has to keep a statement inside that database and away from the server's own settings, its files and its process.

Facts the policy has to respect:

- [`sqlite3_stmt_readonly`](https://sqlite.org/c3ref/stmt_readonly.html) is true for `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, `ATTACH` and `DETACH`, so "read-only" is not a safety check by itself.
- Some pragmas take effect when the statement is compiled, not when it runs, so the policy must decide before or during compilation. The [authorizer](https://sqlite.org/c3ref/set_authorizer.html) is consulted during compilation, which is where it decides.
- Because the authorizer runs at compile time, a compiled statement must never be reused under a different policy.

What the WebAssembly build means. ncruces compiles SQLite to WebAssembly and translates that into Go, so the engine is ordinary Go code with its own linear memory per connection:

- Extension loading does not exist. The build defines `SQLITE_OMIT_LOAD_EXTENSION` ([sqlite_opt.h#L38](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L38)), so neither `load_extension()` nor `sqlite3_load_extension` is compiled in.
- Each connection's memory is capped at 256 MiB by default ([wrap.go#L28](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/wrap.go#L28)); [`WithMaxMemory`](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/wrap.go#L18) can change it when the connection is opened. When SQLite reports `SQLITE_NOMEM`, ncruces panics instead of returning an error ([conn.go#L493-L494](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L493-L494)). SQLite reports `SQLITE_NOMEM` for a statement over the VDBE-operations limit as well ([vdbeaux.c#L187-L188](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vdbeaux.c#L187-L188)), so that limit also ends in a panic.
- The C stack is a fixed 64 KiB region at the start of linear memory ([build.sh#L23](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/build.sh#L23), [sqlite3.go#L33](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/sqlite3.go#L33)). A statement that recurses too deeply drives the stack pointer below zero; the translated code then panics with an index out of range, and every later call on that connection panics too. Measured on this build at SQLite's default limits, on a new connection, it took a chain of about 350 operators (`SELECT 1 + 1 + …`), 225 `UNION ALL` terms, 275 nested subqueries in `FROM`, a `LIKE` pattern of about 9,000 bytes alternating `%` and a character, or a chain of 60 to 80 triggers. SQLite's defaults do not protect this build.
- The configuration the build compiles in: foreign keys are enforced by default ([sqlite_opt.h#L22](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L22)) and `trusted_schema` is off by default ([sqlite_opt.h#L23](https://github.com/ncruces/go-sqlite3-wasm/blob/v6.5.35401/build/sqlite_opt.h#L23)).
- `Conn.Config` rejects database-configuration options above `SQLITE_DBCONFIG_REVERSE_SCANORDER` ([config.go#L19-L20](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/config.go#L19-L20)), so `SQLITE_DBCONFIG_ENABLE_ATTACH_CREATE` and `_ATTACH_WRITE` cannot be set through it. The policy does not need them, because it refuses `ATTACH`.
- The authorizer callback receives a NULL argument as an empty string ([config.go#L221-L222](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/config.go#L221-L222)), so it cannot tell `PRAGMA foreign_keys` from `PRAGMA foreign_keys = ''`, which turns enforcement off. A pragma is therefore allowed or refused by name, whatever its argument.
- ncruces installs a progress handler on every connection that interrupts the running statement, every 1,000 virtual-machine instructions, once the context given to `SetInterrupt` is done ([conn.go#L120](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L120), [conn.go#L351-L358](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/conn.go#L351-L358)).

## Decision

### Where the policy applies

The engine opens each connection, calls `Policy.Apply` once, and runs every client statement through `Conn.Run`. `Run` compiles each statement in the text it is given, runs it, and closes it, so no compiled statement outlives the request it was compiled for, and there is no statement cache. `sqlite3_set_authorizer` also expires every statement already prepared on the connection ([auth.c#L81](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/auth.c#L81)), so a statement prepared before `Apply` is compiled again, under the policy, if it is stepped.

### Mechanisms

| Mechanism | What it is for |
|---|---|
| Authorizer, default-deny ([docs](https://sqlite.org/c3ref/set_authorizer.html)) | Decides, while each statement is compiled, which actions, pragmas and functions it may use. Any action code the policy does not list is refused, including ones a later SQLite adds. |
| `SQLITE_DBCONFIG_DEFENSIVE` on ([docs](https://sqlite.org/c3ref/c_dbconfig_defensive.html#sqlitedbconfigdefensive)) | Disables the language features that can corrupt the file, such as `PRAGMA writable_schema = ON` and `PRAGMA journal_mode = OFF`, as a second line behind the pragma allowlist. |
| `SQLITE_DBCONFIG_WRITABLE_SCHEMA` off | Clears a writable schema set before `Apply`. |
| `trusted_schema` off ([docs](https://sqlite.org/pragma.html#pragma_trusted_schema)) | Views, triggers, `CHECK` constraints, defaults and generated columns may call only functions marked innocuous, so a schema object cannot run a function the client could not call directly. |
| `SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION` off ([docs](https://sqlite.org/c3ref/enable_load_extension.html)) | Kept off although the build omits extension loading, so the setting is right if a later build includes it. |
| `temp_store = MEMORY` ([docs](https://sqlite.org/pragma.html#pragma_temp_store)) | Temporary tables, indices, sorts and statement journals stay in the connection's memory. Otherwise ncruces creates unnamed files in the process's temporary directory ([os_linux.go#L14-L41](https://github.com/ncruces/go-sqlite3/blob/v0.37.0/vfs/os_linux.go#L14-L41)). |
| Run-time limits ([docs](https://sqlite.org/c3ref/limit.html)) | Bound the size of one statement and one value, and keep every nesting that recurses in C inside the 64 KiB stack; see the table below. The number of attached databases is fixed at zero. |
| Time limit | `Run` gives the connection a context that ends at the policy's timeout, and the progress handler interrupts the statement then ([docs](https://sqlite.org/c3ref/interrupt.html)). |
| Panic containment | `Run` recovers the panics ncruces raises for `SQLITE_NOMEM` and for a stack overflow, and marks the connection broken. |

Authorizer actions allowed: `SELECT`, reading a column, recursive common table expressions, `INSERT`, `UPDATE`, `DELETE`, transactions and savepoints, creating and dropping tables, indices, views and triggers (temporary or not), `ALTER TABLE`, `REINDEX`, `ANALYZE`, and the pragmas and functions listed below. Refused: `ATTACH`, `DETACH`, `CREATE VIRTUAL TABLE`, `DROP VIRTUAL TABLE`, and any other action code. Writes to `sqlite_schema` cannot be refused by the authorizer, because SQLite authorizes its own write to `sqlite_schema` with the same code when it creates a table ([build.c#L1320](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/build.c#L1320)); defensive mode and `writable_schema` off refuse them instead.

Limits, each a field of `sqlpolicy.Limits` with this default. New refuses values outside SQLite's compiled range, and Apply reads each limit back.

| Limit | Default | SQLite's default here | Why |
|---|---|---|---|
| `SQLITE_LIMIT_LENGTH` | 16 MiB | 1,000,000,000 | One string or BLOB; SQLite's default exceeds the connection's 256 MiB. |
| `SQLITE_LIMIT_SQL_LENGTH` | 1 MiB | 1,000,000,000 | One statement's text. |
| `SQLITE_LIMIT_EXPR_DEPTH` | 100 | 1,000 | The operator chain above overflows the stack from about 350. The same limit bounds nested views and common table expressions ([select.c#L2497-L2500](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/select.c#L2497-L2500)). |
| `SQLITE_LIMIT_PARSER_DEPTH` | 200 | 2,500 | Nesting in parentheses; 200 allows 31 nested subqueries in `FROM`, against a crash at 275. The parser's first 50 entries are static, so values below 50 act as 50 ([parse.y#L25](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/parse.y#L25), [lempar.c#L306-L311](https://github.com/sqlite/sqlite/blob/version-3.54.1/tool/lempar.c#L306-L311)), and New refuses them. |
| `SQLITE_LIMIT_COMPOUND_SELECT` | 100 | 500 | `UNION ALL` chains overflow the stack from about 225 terms. |
| `SQLITE_LIMIT_LIKE_PATTERN_LENGTH` | 4,096 | 50,000 | Matching recurses once per wildcard; overflow from about 9,000 bytes. |
| `SQLITE_LIMIT_TRIGGER_DEPTH` | 32 | 1,000 | SQLite checks it while it compiles a chain of triggers ([trigger.c#L1259-L1261](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/trigger.c#L1259-L1261)), before the chain reaches the 60 to 80 that overflow the stack. |
| `SQLITE_LIMIT_VDBE_OP` | 1,000,000 | 250,000,000 | Bounds the memory one compiled statement takes. Exceeding it panics, so the connection is then discarded. |
| `SQLITE_LIMIT_FUNCTION_ARG` | 1,000 | 1,000 | Kept; nothing found that it endangers. |
| `SQLITE_LIMIT_VARIABLE_NUMBER` | 32,766 | 32,766 | Kept; ORMs bind one parameter per element of an `IN` list, and the statement-length limit already bounds parsing. |
| `SQLITE_LIMIT_ATTACHED` | 0 | 10 | Fixed, not configurable; a second refusal of `ATTACH` and `VACUUM`. |
| Time limit | 5 s | none | Per `Run`. |

The stack figures are measurements of this build, not properties SQLite documents. The test suite runs each construct at its default limit and one past it, and checks that the connection survives; it has to be run again when ncruces is upgraded.

A limit that SQLite enforces fails the statement with SQLite's own error, which names the limit; the policy passes it through as an engine error and does not parse messages.

When the time limit interrupts a write inside an explicit transaction, SQLite rolls back the whole transaction; an interrupted read leaves the transaction open. Both are tested. If the caller's own context ends first, `Run` returns the engine's `SQLITE_INTERRUPT` error rather than a policy refusal.

After `Run` recovers a panic, it returns `*Error` with reason `memory` for `SQLITE_NOMEM` or an error wrapping `ErrBroken` for anything else, and every later `Run` on that connection returns `ErrBroken`. The engine must then discard the connection. `Conn.Close` tries to close it; after a stack overflow SQLite cannot close it, and its file descriptors and locks are released only when Go finalizes them. No statement at the default limits is known to overflow the stack; containment is for the ones nobody has found.

### Pragmas

Every pragma is refused unless it is on this list. The policy decides by the pragma's name, in any letter case, for `PRAGMA x`, `PRAGMA x = y`, `PRAGMA x(y)` and schema-qualified forms. A pragma's table-valued function, such as `pragma_journal_mode`, compiles the pragma when the statement scans the function ([pragma.c#L3011-L3021](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/pragma.c#L3011-L3021)), and the authorizer refuses it then; a statement whose plan never scans it succeeds without running the pragma.

| Allowed pragma | Why |
|---|---|
| `table_info` | Read-only column list. JDBC `getPrimaryKeys` in the reference driver, sqlite-jdbc ([JDBC3DatabaseMetaData.java#L2020](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L2020)). |
| `table_xinfo` | Read-only column list including hidden columns. sqlite-jdbc `getColumns` ([#L980](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L980)). |
| `index_list` | Read-only. sqlite-jdbc `getIndexInfo` ([#L1543](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L1543)); GORM `GetIndexes` as `PRAGMA_index_list(?)` ([migrator.go#L342](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L342)). |
| `index_info` | Read-only. sqlite-jdbc ([#L1567](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L1567)); GORM ([migrator.go#L350](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L350)). |
| `foreign_key_list` | Read-only. sqlite-jdbc `getImportedKeys` ([#L1430](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L1430)). |
| `quick_check` | Read-only. SQLite runs it itself, through `pragma_quick_check`, when `ALTER TABLE … ADD COLUMN` changes a STRICT table or one with `CHECK` constraints ([alter.c#L472-L487](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/alter.c#L472-L487)), and the authorizer cannot tell that from a client's call. Bounded by the time limit. |

Settings the server owns, which clients may not read or change through a pragma: `journal_mode`, `synchronous`, `locking_mode`, `page_size`, `wal_checkpoint`, `wal_autocheckpoint`, `journal_size_limit`, `fullfsync`, `checkpoint_fullfsync`, `auto_vacuum`, `incremental_vacuum`, `encoding`, `secure_delete`, `max_page_count`, `schema_version`, `user_version`, `application_id`, `writable_schema`, `trusted_schema`, `cell_size_check`, `ignore_check_constraints`, `legacy_alter_table`, `foreign_keys`, `defer_foreign_keys`, `recursive_triggers`, `query_only`, `read_uncommitted`, `temp_store`, `temp_store_directory`, `cache_size`, `cache_spill`, `mmap_size`, `threads`, `soft_heap_limit`, `hard_heap_limit`, `busy_timeout`, `analysis_limit` and every other pragma of the build. Refused introspection includes `database_list`, which shows the database file's path on the server.

### ATTACH, DETACH and VACUUM

`ATTACH` and `DETACH` are refused by the authorizer in every form: a plain or URI filename, `:memory:`, an empty name, or a filename computed by an expression. SQLite does not accept `ATTACH` inside a trigger body or a view, so those are syntax errors. `VACUUM` consults no authorizer action of its own ([vacuum.c#L105-L138](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vacuum.c#L105-L138)); while it runs, it attaches its output with `ATTACH` ([vacuum.c#L226](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/vacuum.c#L226)), the authorizer refuses that, and the attached-database limit of zero would refuse it again ([attach.c#L170](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/attach.c#L170)). So `VACUUM` and `VACUUM INTO` are refused, with reason `attach`; compacting the file belongs to the engine.

### Temporary tables

Clients may create and drop temporary tables, indices, views and triggers, including temporary triggers on their own main tables: they are private to the connection and hold only the client's own data. (Hibernate's SQLite dialect does not use them; it keeps the default of ordinary tables for its bulk-operation tables, [Dialect.java#L4018-L4020](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/dialect/Dialect.java#L4018-L4020).) They live in memory and count against the connection's memory. They last as long as the connection, so a pooled connection that serves a different principal later must be reset; that belongs to the engine's pooling.

### Functions

Every SQL function is refused unless it is on the allowlist in `internal/sqlpolicy`: the core scalar, aggregate, window, date and time, math and JSON functions, and the extensions ncruces registers on every connection (`base64`, `decimal*`, `ieee754*`, `regexp`, `regexpi`). A test lists what ncruces v0.37.0 actually registers, from `pragma_function_list`, and fails if a registered function is neither allowed nor deliberately refused, or if the allowlist names a function that is not registered. Deliberately refused: `sqlite_log`, which writes to the server's log; `sqlite_compileoption_get`, `sqlite_compileoption_used` and `sqlite_source_id`, which describe the server's build; and `match`, which only full-text search uses and this build does not include. Functions that read files or load code (`load_extension`, `readfile`, `writefile`, `edit`, `fts3_tokenizer`) are not in this build, and are refused if a later build or the server registers one. `sqlite_version` stays, because JDBC `DatabaseMetaData` reports the engine's version.

SQLite's internal functions for `ALTER TABLE`, such as `sqlite_rename_table`, are allowed: SQLite calls them from SQL it compiles itself, the authorizer sees them first ([resolve.c#L1204](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/resolve.c#L1204)), and SQLite resolves them only in that SQL ([resolve.c#L1251-L1263](https://github.com/sqlite/sqlite/blob/version-3.54.1/src/resolve.c#L1251-L1263)), so a client calling one gets "no such function".

With `trusted_schema` off, a function that is not marked innocuous, such as `base64`, works in a query but not in a view, trigger or `CHECK` constraint.

### Errors

A refusal is an `*sqlpolicy.Error` with a stable reason code and at most the first 64 bytes of the statement it refused. Everything else is the engine's error, unchanged.

| Reason | Rule |
|---|---|
| `attach` | `ATTACH`, and `VACUUM`, which attaches its output |
| `detach` | `DETACH` |
| `pragma` | A pragma not on the allowlist, in any form |
| `function` | A function not on the allowlist |
| `statement` | Any other action the authorizer refuses, such as `CREATE VIRTUAL TABLE` |
| `time_limit` | The statement ran past the time limit |
| `memory` | The connection's memory, or `SQLITE_LIMIT_VDBE_OP`; the connection is then broken |

### What the two ORMs need

Hibernate ORM 7.4.13 with the community SQLite dialect issues no pragmas ([SQLiteDialect.java](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java)). For the JPA acceptance suite in `clients/java/acceptance`, its schema creation emitted `drop table if exists` and `create table` with inline `unique`, then `select last_insert_rowid()` after each insert of a generated identifier. Its schema validation reads JDBC metadata; skylite's driver does not exist yet, so the test uses the queries the reference driver, sqlite-jdbc 3.53.4.0, runs for those calls. sqlite-jdbc builds `getColumns` as one compound `SELECT` with one term per column of every table that matches ([JDBC3DatabaseMetaData.java#L998](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java#L998)); under this policy that fails past 100 columns, and at SQLite's own limit it would overflow the stack past about 225. skylite's JDBC driver must not build metadata results that way.

GORM v1.31.2 with `gorm.io/driver/sqlite` v1.6.0 runs, for `AutoMigrate`, queries against `sqlite_master` ([migrator.go#L29-L76](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L29-L76), [#L109-L155](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L109-L155), [#L208-L225](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L208-L225), [#L281-L298](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L281-L298)), `CREATE TABLE`, `CREATE INDEX` ([migrator.go#L252-L279](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L252-L279)) and `ALTER TABLE ? ADD ? ?` ([gorm migrator.go#L122-L203](https://github.com/go-gorm/gorm/blob/v1.31.2/migrator/migrator.go#L122-L203), [#L386-L387](https://github.com/go-gorm/gorm/blob/v1.31.2/migrator/migrator.go#L386-L387)); `GetIndexes` uses `PRAGMA_index_list` and `PRAGMA_index_info`. `DropTable` and the table rebuild that `AlterColumn` and `DropColumn` use read and set `PRAGMA foreign_keys` and ignore both errors ([migrator.go#L18-L27](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L18-L27)); the policy refuses both, so those operations run with foreign keys enforced and can fail where a table that other tables reference is dropped or rebuilt. `CurrentDatabase` reads `PRAGMA database_list` ([migrator.go#L227-L231](https://github.com/go-gorm/sqlite/blob/v1.6.0/migrator.go#L227-L231)), which is refused; the SQLite migrator does not call it for `AutoMigrate`.

The test suite runs each of these statements on a database in memory and in a file, and all pass.

### Not in the first version

- A policy per principal, for example read-only clients, and row-level rules.
- Resource accounting per tenant. The per-connection memory cap is the engine's choice when it opens a connection.
- Result size and row count, which belong to the HTTPS layer.
- Resetting a connection's temporary schema between principals, and replacing a broken connection, which belong to the engine's connection handling.

## Alternatives considered

- **Parse SQL with a separate parser and decide on its syntax tree.** Rejected: the policy would have to track SQLite's grammar exactly, and any statement it parsed differently from SQLite would bypass it. The authorizer sees what SQLite actually compiles, including statements SQLite compiles itself, views, triggers and the `ATTACH` inside `VACUUM`.
- **A denylist of dangerous actions, pragmas and functions.** Rejected: SQLite and ncruces add pragmas, functions and authorizer codes between releases, and a denylist would allow each new one until someone noticed. A default-deny allowlist refuses them, and the registry test reports them.
- **`sqlite3_stmt_readonly` or `PRAGMA query_only` as the guard.** Rejected: neither refuses `ATTACH`, pragmas or functions, and `sqlite3_stmt_readonly` is true for `ATTACH`.

## Consequences

- The engine must apply the policy to every connection that runs client SQL and run that SQL only through `Conn.Run`, and must discard a connection after `ErrBroken` or a `memory` refusal.
- skylite's JDBC driver must answer `DatabaseMetaData` without a compound `SELECT` per column.
- GORM's `DropTable` and column changes on tables that other tables reference may fail; an application that needs them must order its migrations or drop the referencing tables first.
- Upgrading ncruces means rerunning the package's tests, which check the function and pragma registries and the stack measurements, and revisiting the limits if they fail.
