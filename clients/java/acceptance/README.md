# JPA acceptance suite

A small Spring Boot application with Spring Data JPA and Hibernate, and tests
that exercise what an ORM does to a database. The JDBC URL, driver class and
Hibernate dialect come from configuration, so the same tests can run against
any JDBC driver. It was written to decide how skylite's JDBC support should be
built (decision record 0003, `docs/content/docs/decisions/0003-jdbc-transport.md`)
and now runs against skylite too, through skylite's JDBC driver and Hibernate
dialect (see "Running against skylite" below).

Build tool: Maven, because the Spring Boot parent POM pins every plugin and
library version in one place and Dependabot updates it directly.

## Running

From the repository root, the baseline (sqlite-jdbc on a file in `target/`):

```sh
make java-acceptance
```

Against another driver, set the three properties on the Maven command line.
This is the only supported way: the Surefire configuration in `pom.xml` passes
them to the tests as system properties of the same names, and
`application.properties` maps them to `spring.datasource.url`,
`spring.datasource.driver-class-name` and `spring.jpa.database-platform`.

```sh
mvn -f clients/java/acceptance/pom.xml test \
  -Djdbc.url=jdbc:... -Djdbc.driver=com.example.Driver \
  -Djdbc.dialect=org.hibernate.community.dialect.SQLiteDialect
```

Spring Boot reads connection settings from many other sources, and most of
them outrank `application.properties`: other system properties (Surefire also
passes every Maven `-D` property to the tests, so
`-Dspring.datasource.url=...` arrives as one), environment variables,
`SPRING_APPLICATION_JSON` and configuration files; Hibernate reads its own
connection settings as well. So each time the application starts,
`ConnectionReport` compares what the application uses with the system
properties `jdbc.url`, `jdbc.driver` and `jdbc.dialect`, read from the JVM and
not from Spring's environment, and fails start-up on any difference. It checks
in two steps, both before Hibernate creates, alters or drops anything:

- when the pool is created, before it opens a connection: that it is HikariCP,
  configured with the requested JDBC URL and driver class rather than a
  `DataSource` class;
- when Hibernate builds its session factory, before it drops or creates any
  table: that Hibernate takes its connections from that pool, that it built the
  requested dialect, and that the driver reports the requested URL for a pooled
  connection (`DatabaseMetaData.getURL()`).

The first step runs before the pool opens a connection. The second does not
stop everything reaching the database: when Hibernate's own connection
settings are overridden, Hibernate connects and reads the database's metadata
before the check rejects the run, as in the `SPRING_APPLICATION_JSON` row with
`connection.provider_class` below.

Connections the tests open outside the pool, to check what another connection
sees, are opened by `ConnectionReport.openOutsidePool()` with the requested
driver class and URL, the values the check verified, and with the pool's
driver properties (`spring.datasource.hikari.data-source-properties.*`, such as
skylite's `tokenFile`), user name and password, if it has any. They do not use `spring.datasource.*`, which
can differ from what the pool uses (see the `hikari.jdbc-url` row below).

The check certifies that the URL, the driver class and the dialect in use are
the requested ones. It does not certify the user name or password. SQLite
ignores them; a driver that authenticates uses them to decide the principal,
and some drivers use the user name to choose a default database or schema
when the URL names none, so for such a driver the same URL can reach
different data.

When start-up fails, every test errors; none is skipped. When it succeeds, the
log names the database the run certified:

```text
ConnectionReport : Connected to jdbc:sqlite:.../target/baseline.db?journal_mode=WAL&busy_timeout=5000 with driver org.sqlite.JDBC (SQLite JDBC 3.53.4.0), dialect org.hibernate.community.dialect.SQLiteDialect, database SQLite 3.53.4; this is the baseline stack
```

Ways of pointing the suite elsewhere, and their results, are listed under
"Configuration that was tried" below; `make java-config-check` runs them again.

The `flightsql` profile runs the suite through the Apache Arrow Flight SQL JDBC
driver against the server in `internal/spike/flightsql`:

```sh
(cd internal/spike/flightsql && go run . -db /tmp/flightsql.db) &
mvn -f clients/java/acceptance/pom.xml test -Pflightsql
```

### Running against skylite

From the repository root, with a JDK, Maven and Go:

```sh
make java-acceptance-skylite
```

It installs `clients/java/driver` and `clients/java/hibernate` into the local
Maven repository, then runs the suite with the `skylite` profile:
`io.github.randomcodespace.skylite.jdbc.SkyliteDriver` with
`io.github.randomcodespace.skylite.hibernate.SkyliteDialect`. Before any test,
`SkyliteServer`, a JUnit launcher session listener that does nothing without
`-Dskylite.serve=true`, builds `cmd/skylite` from this repository and starts
`skylite serve` on a free port of 127.0.0.1, as the driver's integration test
does: HTTPS with a self-signed certificate, JWT authentication by a test
issuer whose ES256 token it signs, and storage on the local file system under
`target/skylite/`. It creates the database `acceptance` with a `PUT`, waits
until the server leads it, and sets the system properties `jdbc.url` and
`spring.datasource.hikari.data-source-properties.tokenFile` and
`.tlsRootCertificates`, which the pool passes to the driver. The server stops
when the run ends; its log is in `target/skylite/run-*/skylite.log`.

28 tests pass and one is skipped, with its reason printed:

```text
SKIPPED TransactionTest.rollbackToSavepointKeepsEarlierWrites: the driver reports no savepoint support; skylite's server refuses SAVEPOINT, RELEASE and ROLLBACK TO on a write channel
```

That test is skipped wherever the verified connection's driver reports
`DatabaseMetaData.supportsSavepoints()` false; sqlite-jdbc supports them, so it
runs on the baseline and in the fault check. The baseline's five known
failures pass on skylite: its dialect declares foreign keys and takes
generated keys from the driver, and its driver reports `BIGINT` for an
`integer` column, implements `getBlob`, and raises
`SQLIntegrityConstraintViolationException` with SQL state `23000` for
`SQLITE_CONSTRAINT`.

### Tests skipped on the baseline

Five tests are known to fail on the baseline stack, sqlite-jdbc with the
Hibernate community SQLite dialect (see "Known baseline failures" below). They
are skipped only when the verified connection above uses the driver
`org.sqlite.JDBC` and the dialect `org.hibernate.community.dialect.SQLiteDialect`;
with any other driver or dialect every test runs, with no flag. The skip
condition reads the result of that check from the application context, so it
cannot apply unless start-up verified the connection. Each skipped test is printed with its
reason, for example:

```text
SKIPPED LobTest.lobRoundTrips: known baseline failure: with the community dialect, Hibernate reads @Lob byte[] with ResultSet.getBlob, which sqlite-jdbc does not implement
```

To run every test on the baseline as well:

```sh
mvn -f clients/java/acceptance/pom.xml test -Dacceptance.include-baseline-failures=true
```

## Configuration

`src/main/resources/application.properties` holds the settings every run
shares: `ddl-auto=create`, with `hibernate.hbm2ddl.halt_on_error=true` so that
a failed `CREATE`, `DROP` or `ALTER` fails start-up instead of being logged and
skipped, JDBC batch size 50 with ordered inserts, and a HikariCP pool of up to
4 connections.

### Configuration that was tried

`make java-config-check` (`config-check.py`, which needs Python 3) runs
`CrudTest` and `TransactionTest` once for each row below, in a copy of the
project under `target/config-check`. Each run requests one SQLite file with
`-Djdbc.url` and, through the source in the row, points the suite at another
SQLite file (and, where noted, at another driver class or dialect). That other
file holds a table `author` with one sentinel row, which Hibernate's
`ddl-auto=create` would drop. The script checks each result and that the
sentinel row is still there afterwards, and exits non-zero on any difference.
It is not part of CI; it took about 13 minutes on an 8-core machine. Its last run was on
2026-11-15, with every row as listed.

"Rejected" means start-up failed with `the application is not connected to the
requested database`, naming the check, and the tests errored. "Requested" means
the tests passed and the log named the requested database: either Spring read
the setting but a higher-priority source (the `jdbc.*` system properties from
Surefire) won, or the setting was not used. As a control, the other file given
as `-Djdbc.url` itself, the requested database, lost its sentinel row.

| Source | Setting | Result |
|---|---|---|
| Maven property, which Surefire passes on as a system property | `spring.datasource.url`, `driver-class-name` and `spring.jpa.database-platform` together, a wrapper driver on the other file, with `jdbc.url` set to a database that does not exist (the bypass found in review) | Rejected (pool JDBC URL) |
| Maven property | `spring.datasource.url` | Rejected (pool JDBC URL) |
| Maven property | `spring.datasource.url` and `driver-class-name`, a wrapper driver on the other file | Rejected (pool JDBC URL) |
| Maven property | `spring.datasource.driver-class-name`, and separately `spring.datasource.hikari.driver-class-name`, set to a wrapper driver | Rejected (pool driver class) |
| Maven property | `spring.datasource.hikari.jdbc-url` | Rejected (pool JDBC URL) |
| Maven property | `spring.datasource.url` set to the other file and `spring.datasource.hikari.jdbc-url` set to the requested database (found in the fourth review) | Requested: the pool used `hikari.jdbc-url`, and the connections the tests open outside the pool use `jdbc.url`. Before `openOutsidePool()`, those connections followed `spring.datasource.url` to the other file and `TransactionTest` failed there |
| Maven property | `spring.jpa.database-platform`, and separately `spring.jpa.properties.hibernate.dialect`, set to another dialect | Rejected (Hibernate dialect) |
| Maven property | `spring.datasource.type` set to Spring's `SimpleDriverDataSource` | Rejected (not HikariCP) |
| Maven property | Hibernate's own `hibernate.connection.provider_class` with `hibernate.connection.url`, or with `jakarta.persistence.jdbc.url` | Start-up failed in Hibernate, which took the provider class but no URL from system properties; no connection was opened |
| Maven property | `spring.jpa.properties.hibernate.connection.provider_class` with `spring.jpa.properties.hibernate.connection.url`, or with `spring.jpa.properties.jakarta.persistence.jdbc.url` | Rejected (Hibernate not using the pool) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.url` | Rejected (pool JDBC URL) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.url` and `driver-class-name`, a wrapper driver on the other file | Rejected (pool JDBC URL) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.driver-class-name` set to a wrapper driver | Rejected (pool driver class) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.hikari.jdbc-url` | Rejected (pool JDBC URL) |
| `SPRING_APPLICATION_JSON` | `jdbc.url`, which `application.properties` refers to | Rejected (pool JDBC URL) |
| `SPRING_APPLICATION_JSON` | `spring.jpa.database-platform`, and separately `spring.jpa.properties.hibernate.dialect` | Rejected (Hibernate dialect) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.type` set to Spring's `SimpleDriverDataSource` | Rejected (not HikariCP) |
| `SPRING_APPLICATION_JSON` | Hibernate's own `connection.provider_class` and `connection.url` | Rejected (Hibernate not using the pool); Hibernate had connected to the other file to read its metadata, and the check stopped it before any DDL |
| Environment variable | `SPRING_DATASOURCE_URL`, and separately `SPRING_DATASOURCE_HIKARI_JDBC_URL` | Rejected (pool JDBC URL) |
| Environment variable | `SPRING_DATASOURCE_DRIVER_CLASS_NAME` set to a wrapper driver | Rejected (pool driver class) |
| Environment variable | `SPRING_DATASOURCE_TYPE` set to Spring's `SimpleDriverDataSource` | Rejected (not HikariCP) |
| Environment variable | `SPRING_JPA_DATABASE_PLATFORM` set to another dialect | Rejected (Hibernate dialect) |
| Environment variable | `JDBC_URL` | Requested: outranked |
| `JAVA_TOOL_OPTIONS` | `-Dspring.datasource.url=...` | Rejected (pool JDBC URL) |
| `JAVA_TOOL_OPTIONS` | `-Djdbc.url=...` | Requested: outranked, Surefire sets the system property again |
| Extra JVM argument (`-Djdbc.argLine`) | `-Dspring.datasource.url=...` | Rejected (pool JDBC URL) |
| Extra JVM argument (`-Djdbc.argLine`) | `-Djdbc.url=...` | Requested: outranked, as above |
| `spring.config.additional-location` | a file with `spring.datasource.url`, and separately one with `spring.datasource.hikari.jdbc-url` | Rejected (pool JDBC URL) |
| Profile-specific file on the class path (`application-review.properties`, profile activated) | `spring.datasource.url` | Rejected (pool JDBC URL) |
| `application.properties` in the working directory | `spring.datasource.url` | Rejected (pool JDBC URL) |
| `config/application.properties` in the working directory | `spring.datasource.url` | Rejected (pool JDBC URL) |

Command-line arguments to the application cannot be passed through Surefire:
the tests start the application themselves.

## What each test stands for

| Test | What it checks | Why an ORM needs it |
|---|---|---|
| `SchemaLifecycleTest.createThenValidate` | Start with `ddl-auto=create`, write a row, stop; start again with `ddl-auto=validate` and read the row back | Hibernate generates DDL from the entities, and `validate` compares the entities with the database's JDBC metadata (`getTables`, `getColumns`); applications commonly validate on start |
| `CrudTest.createReadUpdateDelete` | Insert two rows with `IDENTITY` ids and read each back by its id, update and delete one; the other is the only row left | Database-generated ids reach Hibernate through `getGeneratedKeys`, `RETURNING` or an identity select, depending on dialect and driver |
| `RelationsTest.lazyAssociationsLoadInsideTransaction` | `@OneToMany` and lazy `@ManyToOne` are uninitialised after load and load on first access inside a transaction, with the right author and titles | Lazy loading issues further queries on the same connection and persistence context |
| `RelationsTest.joinFetchLoadsParentAndChildren` | A JPQL `join fetch` loads an author and its books in a fresh persistence context; the collection is initialised, each book has the id and title it was saved with, and each points to the same author instance | One result row holds columns of two tables, with several identifier columns, which Hibernate reads by alias and assembles into a parent and its children |
| `RelationsTest.cascadeAndOrphanRemovalDeleteBooks` | Removing a book from an author's collection deletes it, and deleting the author deletes its remaining books, checked from a new connection | `CascadeType.ALL` with `orphanRemoval` turns collection changes into `delete` statements |
| `TransactionTest.commitIsVisibleFromSecondConnection` | A `@Transactional` service method saves an author and then each book in separate repository calls; a new connection outside the pool sees all rows | One transaction spans several statements, and a commit must make them all durable together |
| `TransactionTest.exceptionRollsBackEveryWrite` | The same writes, flushed, then an exception; a new connection outside the pool sees no rows | Spring rolls back on a runtime exception; a driver whose `rollback()` does nothing would leave partial writes behind |
| `TransactionTest.rollbackToSavepointKeepsEarlierWrites` | On one pooled connection: insert, `setSavepoint`, insert, `rollback(savepoint)`, commit; only the first row remains | Spring's nested transactions use JDBC savepoints |
| `TransactionTest.failedStatementLeavesTransactionUsable` | Inside a transaction, an insert that breaks a unique constraint fails with `SQLException`; the transaction's other inserts, before and after it, commit | An application may catch a failed statement and carry on in the same transaction; SQLite undoes only the failed statement |
| `QueryTest.derivedQuery` | `findByNameStartingWithOrderByNameAsc` returns the two matching names in order; `findByEmail` returns the author with that email, and returns nothing for an email that no row has, while the table holds 7 rows | Spring Data turns method names into JPQL with `LIKE` and `ORDER BY` |
| `QueryTest.jpqlQuery` | `@Query` with `size()` on a collection | JPQL is translated to SQL with a correlated subquery |
| `QueryTest.nativeQuery` | `@Query(nativeQuery = true)` with a named parameter | Native SQL goes to the driver as written |
| `QueryTest.paginationAndSorting` | `findAll(PageRequest.of(2, 2, Sort...))`: limit 2 and offset 4, which differ, so swapping them returns another page | Paging issues `LIMIT`/`OFFSET` from the dialect plus a `count` query |
| `QueryTest.bulkUpdateReportsAffectedRows` | A JPQL bulk update returns 3, 1 and 0 for updates of three rows, one row and a missing row, and every row then holds the expected name | `@Modifying` queries return the driver's update count from `executeUpdate`; Hibernate's own entity updates go through `executeBatch` instead |
| `QueryTest.duplicateColumnLabelReadsTheFirstColumn` | In a join whose two columns share the label `name`, `findColumn("name")` is 1 and `getString("name")` is the first column's value | JDBC returns the first matching column for a label ([`java.sql.ResultSet`](https://docs.oracle.com/en/java/javase/25/docs/api/java.sql/java/sql/ResultSet.html)); code that reads by label relies on it |
| `OptimisticLockingTest.versionIncrementsAndStaleUpdateFails` | `@Version` increments by one, and saving a stale detached copy raises `ObjectOptimisticLockingFailureException` | Saving a detached copy goes through Hibernate's merge, which loads the current row and rejects the version before any `UPDATE` is sent; this test does not reach the driver's update count |
| `OptimisticLockingTest.updateMatchingNoRowFailsOnFlush` | Inside a transaction, a managed account is loaded, its stored version is advanced by plain JDBC on the same connection, and the entity is changed and flushed; the flush raises `ObjectOptimisticLockingFailureException` and the row keeps its balance | The flush sends `update ... where id = ? and version = ?`, which matches no row; only the update count of 0 from the driver tells Hibernate the update failed. A driver that reports 1 for every update fails this test |
| `BatchInsertTest.insertsAreBatched` | 500 inserts with JDBC batching on; Hibernate prepares the insert at most once per batch of 50; read back through a fresh persistence context, the (id, sequence value) pairs are exactly the 500 that were saved, and in `rowid` order the sequence values are in the order they were added to the batch | Batching uses `addBatch`/`executeBatch` on a prepared statement; ids are application-generated UUIDs because Hibernate does not batch `IDENTITY` inserts |
| `ColumnTypesTest.valuesRoundTrip` | String, long, double, boolean, `BigDecimal`, `Instant`, `LocalDate` and `byte[]` in a `blob` column round-trip in three rows: the top of each range (`Long.MAX_VALUE`, a string with an apostrophe, accented letters, `✓` and `😀`, now, a 300 KiB payload); the bottom (`Long.MIN_VALUE`, `-Double.MAX_VALUE`, a negative decimal, a string of 90,000 characters, a date and a time in 1969, 3 bytes); and small values (-42, `-Math.E`, -0.01, `false`, the epoch, an empty string, an empty payload). Each payload read back is compared with a copy taken before saving, since the driver receives the entity's own array. An `int` column, in `Event`, round-trips `Integer.MIN_VALUE`, -7 and `Integer.MAX_VALUE` | Each Java type goes through a dialect type mapping and a specific `ResultSet` getter; drivers that convert values get signs, ranges, long strings, characters outside 16 bits and dates before 1970 wrong |
| `ColumnTypesTest.nullsRoundTrip` | The same columns, all null | Binding null needs the right SQL type from the dialect |
| `ColumnTypesTest.parameterReboundToNullBindsNull` | One prepared statement inserts a string and a long, then, with both parameters set to null, inserts again; the second row is null in both | Applications and Hibernate reuse prepared statements; a driver that skips `setNull` on a parameter already set sends the old value |
| `LobTest.lobRoundTrips` | A 300 KiB, an empty and a null `@Lob byte[]` round-trip; the 300 KiB value read back is compared with a copy taken before saving | With the community dialect, Hibernate reads this mapping through `ResultSet.getBlob`; see "Known baseline failures" |
| `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation` | A duplicate unique value raises Spring's `DataIntegrityViolationException`, and behind it the driver's exception is a `SQLIntegrityConstraintViolationException` with an SQL state of class 23 | Applications catch this exception; Hibernate has to recognise the driver's error as a constraint violation. Hibernate accepts either the class or the state, so the test checks both itself: the JDBC API specifies that class for SQL state class 23 ([`SQLIntegrityConstraintViolationException`](https://docs.oracle.com/en/java/javase/25/docs/api/java.sql/java/sql/SQLIntegrityConstraintViolationException.html)) |
| `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation` | Saving a book whose author id does not exist raises the same exceptions, and no book is stored | The schema Hibernate generates must declare the foreign key, and the database must enforce it |
| `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation` | A JPQL update that sets a `nullable = false` column to null raises the same exceptions, and the row is unchanged | Hibernate checks nulls itself before an insert, but not in a JPQL update, so here the database's `not null` constraint and the error conversion are tested |
| `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` | Two HikariCP connections held at once; an uncommitted insert on one is not visible on the other, and rollback removes it | A pool shares one database between threads, which relies on transaction isolation |
| `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection` | Two pooled connections held open: the reader, in autocommit, reads 0; the writer inserts and calls `commit()`; the reader then reads 1, before either connection is closed or has its autocommit reset | Closing a connection or resetting its autocommit also commits, so tests that check from a new connection cannot tell a working `commit()` from one that does nothing; a reader that keeps returning its first result is caught too |
| `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` | A row with id 2147483647 (`Integer.MAX_VALUE`) is inserted first, so every key that follows needs more than 32 bits. Two statements on one pooled connection insert, then a statement on a second connection; only then are the keys read, the first statement's first, and each statement's `getGeneratedKeys` returns the id of its own row, above `Integer.MAX_VALUE` | Identity ids reach Hibernate through the statement that inserted; a driver that asks for "the last inserted id" of the connection, or of the server, returns another statement's id |
| `MetadataTest.productNameAndVersionMatchTheEngine` | `DatabaseMetaData` reports the product name `SQLite`, and a product version and major and minor version that match `select sqlite_version()` | Anything that chooses behaviour by product or version is misled by a driver that misreports them; the test pins both to the engine the connection actually reaches |

## Faults the suite is known to catch

Passing this suite is not a proof that a driver is correct. It is evidence that
the driver does not have any of the faults below, each of which, injected on
its own, made at least one test fail. A fault that is not in this list, or two
faults that cancel each other out, may pass.

`make java-fault-check` runs the check (`src/test/java/.../selfcheck/`).
`FaultyDriver` is a JDBC driver for `jdbc:faulty:jdbc:sqlite:...` URLs that
wraps sqlite-jdbc. It first repairs the five known baseline failures on the
JDBC side (it turns foreign keys on and declares the `book` foreign key,
reports `id` columns as `BIGINT`, implements `getBlob`, and raises constraint
errors as `SQLIntegrityConstraintViolationException` with SQL state 23000), so
that with no fault all 29 tests run and pass. Then it injects the one fault
named by `-Dacceptance.fault`. `FaultCheck` runs the whole suite in a separate
JVM once with no fault, which must pass every test with none skipped, and once
per fault, which must fail at least one test and must have applied the fault
at least once. A fault that breaks DDL starts from a copy of the control
run's database, which already holds the schema, so that only
`halt_on_error` can make it fail: with `halt_on_error=false`,
`ddl-unsupported` was applied 36 times and failed no test. The
`java-acceptance` CI job runs the check after the baseline; it took 266
seconds on an 8-core machine. `-Dacceptance.faults=a,b` runs a subset.

The table below is from the run on 2026-11-15 at commit e51dde4. "Applied" is
how many times the fault changed a call or its result; each run has its own
database file.

| Fault | What the wrapper does | Applied | Tests failed | Caught by |
|---|---|---|---|---|
| `commit-noop` | `commit()` returns without committing | 283 | 1 of 29 | `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection` |
| `rollback-noop` | `rollback()` returns without rolling back | 7 | 2 of 29 | `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection`, `TransactionTest.exceptionRollsBackEveryWrite` |
| `autocommit-ignored` | `setAutoCommit(false)` is ignored, `getAutoCommit()` reports what was asked, `commit()` and `rollback()` do nothing | 579 | 2 of 29 | `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection`, `TransactionTest.exceptionRollsBackEveryWrite` |
| `stale-read` | On a connection in autocommit, a repeated query with the same parameters returns the values it returned the first time | 1 | 1 of 29 | `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection` |
| `dirty-read` | Connections share SQLite's cache with `read_uncommitted`, so one reads another's uncommitted rows | 19 | 1 of 29 | `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` |
| `update-count-one` | `executeUpdate` and `getUpdateCount` return 1 | 165 | 1 of 29 | `QueryTest.bulkUpdateReportsAffectedRows` |
| `update-count-zero` | `executeUpdate` and `getUpdateCount` return 0 | 150 | 4 of 29 | `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection`, `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert`, `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection`, `QueryTest.bulkUpdateReportsAffectedRows` |
| `batch-count-one` | Every count from `executeBatch` is 1 | 1 | 1 of 29 | `OptimisticLockingTest.updateMatchingNoRowFailsOnFlush` |
| `batch-drop-row` | The first row of each batch is not executed; the counts still cover every row | 11 | 2 of 29 | `BatchInsertTest.insertsAreBatched`, `ColumnTypesTest.valuesRoundTrip` |
| `batch-wrong-values` | In a batch, every `setInt` value is bound as 0 | 502 | 2 of 29 | `BatchInsertTest.insertsAreBatched`, `ColumnTypesTest.valuesRoundTrip` |
| `batch-reordered` | The rows of each batch are executed in reverse order | 11 | 1 of 29 | `BatchInsertTest.insertsAreBatched` |
| `generated-key-wrong` | Generated keys, and results of inserts and `last_insert_rowid()`, are 1000 too high | 30 | 19 of 29 | 19 tests, including `CrudTest.createReadUpdateDelete` and `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` |
| `generated-key-other-connection` | `getGeneratedKeys` returns the keys of the most recent insert on any connection | 1 | 1 of 29 | `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` |
| `long-truncated` | `getLong` truncates to an `int` | 2 | 2 of 29 | `ColumnTypesTest.valuesRoundTrip`, `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` |
| `double-rounded` | `getDouble` rounds to `float` precision | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `boolean-inverted` | `getBoolean` returns the opposite | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `decimal-scale-lost` | `getBigDecimal` rounds to scale 0 | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `decimal-precision-lost` | `getBigDecimal` rounds to 6 significant digits | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `instant-shifted` | `getTimestamp` and `getObject` for date-times add two hours | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `date-off-by-one` | `getDate` and `getObject` for dates return the day before | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `string-non-ascii-corrupted` | `getString` replaces non-ASCII characters with `?` | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `empty-string-as-null` | `getString` returns null for an empty string, and `wasNull()` agrees | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `null-as-empty-string` | `getString` returns an empty string for null, and `wasNull()` agrees | 2 | 2 of 29 | `ColumnTypesTest.nullsRoundTrip`, `ColumnTypesTest.parameterReboundToNullBindsNull` |
| `blob-altered` | `getBytes` and `getBlob` flip one bit | 502 | 3 of 29 | `BatchInsertTest.insertsAreBatched`, `ColumnTypesTest.valuesRoundTrip`, `LobTest.lobRoundTrips` |
| `blob-truncated` | `getBytes` and `getBlob` drop the last byte | 502 | 3 of 29 | `BatchInsertTest.insertsAreBatched`, `ColumnTypesTest.valuesRoundTrip`, `LobTest.lobRoundTrips` |
| `blob-wrong-row` | `getBytes` and `getBlob` return the bytes read before, from another row | 502 | 3 of 29 | `BatchInsertTest.insertsAreBatched`, `ColumnTypesTest.valuesRoundTrip`, `LobTest.lobRoundTrips` |
| `params-swapped` | Parameters 1 and 2 of a prepared statement are bound to each other's position | 1094 | 23 of 29 | 23 tests, including `CrudTest.createReadUpdateDelete` and `BatchInsertTest.insertsAreBatched` |
| `null-param-as-zero-or-empty` | `setNull` and a null `setObject` bind `''` for character types, empty bytes for binary types and 0 otherwise | 12 | 4 of 29 | `ColumnTypesTest.nullsRoundTrip`, `ColumnTypesTest.parameterReboundToNullBindsNull`, `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation`, `LobTest.lobRoundTrips` |
| `limit-ignored` | `limit ?` becomes no limit | 1 | 1 of 29 | `QueryTest.paginationAndSorting` |
| `offset-ignored` | `offset ?` becomes offset 0 | 1 | 1 of 29 | `QueryTest.paginationAndSorting` |
| `order-by-ignored` | `order by` is removed from queries, so rows come in storage order | 6 | 3 of 29 | `QueryTest.derivedQuery`, `QueryTest.jpqlQuery`, `QueryTest.paginationAndSorting` |
| `result-stops-early` | `next()` returns false after 100 rows | 1 | 1 of 29 | `BatchInsertTest.insertsAreBatched` |
| `duplicate-rows` | Each result set returns its first row twice | 129 | 23 of 29 | 23 tests, including `BatchInsertTest.insertsAreBatched` and `ColumnTypesTest.valuesRoundTrip` |
| `label-last-column` | `findColumn` and getters by label use the last column with that label, not the first | 1 | 1 of 29 | `QueryTest.duplicateColumnLabelReadsTheFirstColumn` |
| `constraint-wrong-state` | A constraint error is a `SQLIntegrityConstraintViolationException` with SQL state 42000 (syntax error): the class is right, the state is not | 4 | 3 of 29 | `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation`, `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation`, `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation` |
| `error-swallowed` | A failing `execute`, `executeUpdate` or `executeBatch` reports success (count 1) | 4 | 4 of 29 | `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation`, `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation`, `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation`, `TransactionTest.failedStatementLeavesTransactionUsable` |
| `product-name-wrong` | `getDatabaseProductName()` returns `PostgreSQL` | 13 | 1 of 29 | `MetadataTest.productNameAndVersionMatchTheEngine` |
| `product-version-wrong` | `getDatabaseProductVersion()` returns `3.8.0` and `getDatabaseMinorVersion()` 8 | 25 | 1 of 29 | `MetadataTest.productNameAndVersionMatchTheEngine` |
| `ddl-unsupported` | Every `CREATE`, `DROP` and `ALTER` throws `SQLFeatureNotSupportedException`; the run starts from a copy of the control run's database, so the schema already exists | 2 | 29 of 29 | every test: start-up fails |
| `uuid-altered` | `getBytes` flips one bit of every 16-byte value, such as a UUID id | 503 | 2 of 29 | `BatchInsertTest.insertsAreBatched`, `ColumnTypesTest.valuesRoundTrip` |
| `pagination-params-swapped` | In a query with `limit ?` and `offset ?`, the two parameters are bound to each other's position | 2 | 1 of 29 | `QueryTest.paginationAndSorting` |
| `multirow-update-count-one` | `executeUpdate` and `getUpdateCount` return 1 when more than one row changed | 23 | 1 of 29 | `QueryTest.bulkUpdateReportsAffectedRows` |
| `null-rebind-ignored` | `setNull` on a parameter already set in that statement is ignored, so the old value is sent again | 2 | 1 of 29 | `ColumnTypesTest.parameterReboundToNullBindsNull` |
| `quote-string-corrupted` | Apostrophes are removed from string parameters | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `int-truncated-to-short` | `getInt` truncates to a `short` | 2 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `negative-numeric-absolute` | Negative `int`, `long`, `double` and `BigDecimal` values are read as their absolute value | 3 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `pre-epoch-shifted` | Timestamps and dates before 1970 are read one day later | 2 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `null-lob-as-empty` | `getBlob` returns an empty blob for null | 1 | 1 of 29 | `LobTest.lobRoundTrips` |
| `empty-binary-as-null` | `getBytes` and `getBlob` return null for an empty value, and `wasNull()` agrees | 2 | 2 of 29 | `ColumnTypesTest.valuesRoundTrip`, `LobTest.lobRoundTrips` |
| `constraint-wrong-class` | A constraint error is a plain `SQLException` with SQL state 23000: the state is right, the class is not | 4 | 3 of 29 | `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation`, `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation`, `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation` |
| `generated-key-last-on-connection` | `getGeneratedKeys` returns the keys of the most recent insert on the same connection, by any statement | 1 | 1 of 29 | `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` |
| `string-supplementary-corrupted` | `getString` replaces characters outside the 16-bit range, such as `😀`, with U+FFFD | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `long-string-truncated` | `getString` truncates strings to 65,535 characters | 1 | 1 of 29 | `ColumnTypesTest.valuesRoundTrip` |
| `setbytes-mutates-input` | `setBytes` flips one bit of the caller's own array, longer than 16 bytes, before passing it on, so the changed value is stored | 4 | 2 of 29 | `ColumnTypesTest.valuesRoundTrip`, `LobTest.lobRoundTrips` |
| `generated-key-truncated-to-int` | Generated keys, and results of inserts and `last_insert_rowid()`, are truncated to an `int` | 1 | 1 of 29 | `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` |
| `email-filter-ignored` | In a query on `author` filtered by `email = ?`, the filter is replaced, so the first row (smallest `rowid`) comes back whatever the email | 5 | 1 of 29 | `QueryTest.derivedQuery` |

Notes on how the faults were expressed:

- `commit-noop` passed every other test: closing a pooled connection, or
  resetting its autocommit, commits as well, which hides the missing commit
  from any test that checks from a new connection.
- `update-count-one` was caught only by the JPQL bulk update. The fault that
  reached `OptimisticLockingTest` was `batch-count-one`, so Hibernate sent
  that entity update through `executeBatch`.
- `batch-reordered` changes nothing but the order in which rows are inserted,
  so it is caught only through SQLite's `rowid`, which numbers rows in
  insertion order.
- `stale-read` replays results only on connections in autocommit; in this
  run only the JDBC-level reader in `ConnectionPoolTest` met it.
- `uuid-altered`, `blob-altered`, `blob-truncated` and `blob-wrong-row` are
  also caught by `BatchInsertTest`, because `Event` ids are UUIDs stored as
  16-byte blobs and the test compares each id read back with the one saved.
- `constraint-wrong-state` and `constraint-wrong-class` each still reach the
  application as Spring's `DataIntegrityViolationException`: Hibernate
  recognises a constraint violation by the exception class or by the SQL
  state. Only the test's own check of the driver's exception catches them.
- `negative-numeric-absolute` changes nothing for `Long.MIN_VALUE` and
  `Integer.MIN_VALUE`, whose absolute values overflow to themselves; the
  smaller negative values catch it.
- `setbytes-mutates-input`, `generated-key-truncated-to-int` and
  `email-filter-ignored` each failed no test before the change that added
  them: the round trips compared the value read with the array that had been
  saved, which the fault had changed too; no generated key needed more than
  32 bits; and `QueryTest` checked only that the lookup by email returned
  something.
- `long-truncated` is also caught by the generated-key test, whose keys read
  with `getLong` need more than 32 bits.
- Each fault is injected alone. Combinations were not tried.

## Known baseline failures

Five tests fail on the baseline (Spring Boot 4.1.2, Hibernate ORM 7.4.13,
sqlite-jdbc 3.53.4.0). They show requirements on a driver and dialect together,
not defects of one side: for each one, the notes below say which side could
satisfy it, and, for the three constraint tests, what the suite itself requires
of the driver. Those notes were read from the pinned source and were not tested,
except where a test result is quoted.

- `SchemaLifecycleTest.createThenValidate`: validation fails with
  `wrong column type encountered in column [id] in table [account]; found
  [integer (Types#INTEGER)], but expecting [bigint (Types#BIGINT)]`. The dialect
  creates `IDENTITY` columns as `integer`
  ([SQLiteIdentityColumnSupport.java#L29-L32](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/identity/SQLiteIdentityColumnSupport.java#L29-L32)),
  and validation compares the entity's type code with the type code the driver
  reports from `DatabaseMetaData.getColumns`, through the dialect's
  `equivalentTypes`
  ([ColumnDefinitions.java#L28-L30](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/tool/schema/internal/ColumnDefinitions.java#L28-L30)),
  which accepts a wider database type but not a narrower one
  ([Dialect.java#L1868-L1881](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/dialect/Dialect.java#L1868-L1881)).
  Either side could satisfy it: a dialect that treats an `integer` column as
  equivalent to `bigint`, or a driver that reports such a column as `BIGINT`.
- `LobTest.lobRoundTrips`: fails with `not implemented by SQLite JDBC driver`.
  For a `@Lob byte[]`, the community dialect registers
  `BlobJdbcType.PRIMITIVE_ARRAY_BINDING` for the BLOB JDBC type
  ([SQLiteDialect.java#L367](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L367)),
  which binds with `setBytes`
  ([BlobJdbcType.java#L137-L164](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BlobJdbcType.java#L137-L164))
  and reads with `ResultSet.getBlob`
  ([BlobJdbcType.java#L58-L62](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BlobJdbcType.java#L58-L62)),
  and sqlite-jdbc does not implement `getBlob`
  ([JDBC4ResultSet.java#L451-L453](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc4/JDBC4ResultSet.java#L451-L453)).
  Either side could satisfy it: a driver that implements `getBlob`, with the
  community dialect unchanged, or a dialect that registers
  `BlobJdbcType.MATERIALIZED`, which reads with `getBytes`
  ([BlobJdbcType.java#L256-L262](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BlobJdbcType.java#L256-L262)).
  A `byte[]` without `@Lob` in a `blob` column works on the baseline and is
  covered by `ColumnTypesTest`.
- `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation` and
  `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation`: SQLite rejects
  the write (`[SQLITE_CONSTRAINT_UNIQUE]` and `[SQLITE_CONSTRAINT_NOTNULL]`), but
  Spring raises `JpaSystemException` instead of `DataIntegrityViolationException`.
  sqlite-jdbc raises a plain `SQLException` with error code 19
  (`SQLITE_CONSTRAINT`) and no SQL state
  ([SQLiteException.java#L33](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/SQLiteException.java#L33)).
  Hibernate tries the dialect's conversion first, then the exception class, then
  the SQL state
  ([JdbcEnvironmentImpl.java#L315-L321](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/engine/jdbc/env/internal/JdbcEnvironmentImpl.java#L315-L321)),
  and throws `GenericJDBCException` when none matches
  ([StandardSQLExceptionConverter.java#L32-L40](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/StandardSQLExceptionConverter.java#L32-L40)).
  The community dialect's conversion does not handle code 19
  ([SQLiteDialect.java#L451-L470](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L451-L470)).
  What makes Spring translate the error correctly: any one of a dialect that
  maps code 19, a driver that raises `SQLIntegrityConstraintViolationException`
  ([SQLExceptionTypeDelegate.java#L55-L60](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLExceptionTypeDelegate.java#L55-L60)),
  or a driver that reports a class 23 SQL state
  ([SQLStateConversionDelegate.java#L64-L77](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLStateConversionDelegate.java#L64-L77)).
  Each makes Hibernate throw its `ConstraintViolationException`, which Spring
  turns into `DataIntegrityViolationException`
  ([HibernateExceptionTranslator.java#L165-L168](https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L165-L168)).
  What the suite requires is narrower, and only the driver can meet it: the
  driver must raise `SQLIntegrityConstraintViolationException` and give it an
  SQL state of class 23, both. A dialect mapping alone, or only one of the
  class and the state, gives Spring's exception but fails these tests (the
  faults `constraint-wrong-state` and `constraint-wrong-class` show this).
- `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation`: the insert of
  a book with a missing author succeeds, for two separate reasons, both checked
  on the baseline database. The generated `book` table has no foreign key
  (`pragma_foreign_key_list('book')` is empty): the community dialect reports
  no `ALTER TABLE` support
  ([SQLiteDialect.java#L479-L483](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L479-L483)),
  and Hibernate then emits no foreign keys
  ([StandardForeignKeyExporter.java#L37-L42](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/tool/schema/internal/StandardForeignKeyExporter.java#L37-L42)).
  And `PRAGMA foreign_keys` returned 0 on a connection opened with the
  baseline URL, so SQLite would not enforce a foreign key on these connections
  even if the table declared one; sqlite-jdbc sets that pragma only when it is
  configured
  ([SQLiteConfig.java#L796-L798](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/SQLiteConfig.java#L796-L798)).
  Satisfying it needs both sides: a dialect that declares the foreign key in
  `CREATE TABLE` (the community dialect has no syntax for adding one to an
  existing table,
  [SQLiteDialect.java#L500-L508](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L500-L508)),
  and a driver or server that turns enforcement on for every connection. As
  for the other constraint tests, the suite also requires the driver to raise
  `SQLIntegrityConstraintViolationException` with an SQL state of class 23.

## Known baseline limits

- `Instant` values round-trip at millisecond precision; with microseconds the
  test failed. sqlite-jdbc's date precision setting offers seconds or
  milliseconds only
  ([SQLiteConfig.java#L1159-L1161](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/SQLiteConfig.java#L1159-L1161)),
  which is the likely cause; this was inferred, not traced.
- `BigDecimal` is tested with 12 significant digits and scale 2. Higher
  precision was not tested.
- The baseline database is a file in WAL mode with a busy timeout, set in the
  JDBC URL, so that a reader does not block on the other connection's write.
- With the community dialect, an entity with an `IDENTITY` id and an
  `Integer` field gets a `create table` statement whose `id` column has no
  type (`... created_at timestamp, id, number bigint, ...`), seen on
  2026-11-15 with Hibernate ORM 7.4.13 when an `Integer` field was added to
  `Sample`. SQLite then does not make `id` an alias of `rowid`, so ids stay
  null and every lookup by id fails. The cause was not traced. The `int`
  column is therefore tested in `Event`, which has a UUID id.
- `Statement.setQueryTimeout(1)` did not stop a CPU-bound query (an unbounded
  recursive CTE) within 30 seconds on sqlite-jdbc 3.53.4.0, so statement
  timeouts cannot be tested deterministically on the baseline.

## Not covered

The suite does not test these, so a driver can get them wrong and still pass:

- what a batch does when one of its rows fails: which rows are applied, and
  the update counts and `BatchUpdateException` it reports;
- statement timeouts and `Statement.cancel` (see "Known baseline limits");
- Spring's nested transactions (`PROPAGATION_NESTED`); savepoints are tested
  through JDBC only;
- a transaction rolled back after an error, followed by more work on the same
  pooled connection, beyond what consecutive tests do anyway;
- `BigDecimal` beyond 12 significant digits and scale 2, and `Instant` beyond
  millisecond precision;
- the scale of a `BigDecimal`: decimals are compared by value, because
  SQLite's numeric storage does not keep trailing zeros (`1.20` reads back
  as `1.2`);
- special `double` values (`NaN`, infinities, `-0.0`), dates before the
  Gregorian calendar's adoption, and strings with embedded NUL characters;
- whether a unique, foreign-key or not-null violation carries the specific SQL
  state (23505, 23503, 23502) rather than any state of class 23;
- `LocalDateTime` and `OffsetDateTime` columns, enums, embeddables,
  `@ElementCollection`, inheritance, many-to-many associations;
- pessimistic locking, isolation levels other than the default, and
  concurrency beyond two connections held by one thread;
- connection loss and reconnection, TLS, authentication and performance.
