# JPA acceptance suite

A small Spring Boot application with Spring Data JPA and Hibernate, and tests
that exercise what an ORM does to a database. The JDBC URL, driver class and
Hibernate dialect come from configuration, so the same tests can run against
any JDBC driver. It was written to decide how skylite's JDBC support should be
built (decision record 0003, `docs/content/docs/decisions/0003-jdbc-transport.md`)
and is meant to run later against skylite's own endpoint.

Build tool: Maven, because the Spring Boot parent POM pins every plugin and
library version in one place and Dependabot updates it directly.

## Running

From the repository root, the baseline (sqlite-jdbc on a file in `target/`):

```sh
make java-acceptance
```

Against another driver, set the three properties on the Maven command line.
This is the only supported way: the Surefire configuration in `pom.xml` passes
them to the tests as the system properties `spring.datasource.url`,
`spring.datasource.driver-class-name` and `spring.jpa.database-platform`.

```sh
mvn -f clients/java/acceptance/pom.xml test \
  -Djdbc.url=jdbc:... -Djdbc.driver=com.example.Driver \
  -Djdbc.dialect=org.hibernate.community.dialect.SQLiteDialect
```

Spring Boot reads connection settings from other sources too, and some of
them, such as `SPRING_APPLICATION_JSON`, outrank system properties. So each time
the application starts, `ConnectionReport` compares what the application is
actually connected to with those three system properties, read from the JVM and
not from Spring's environment, and fails start-up on any difference. It checks:

- the URL the driver reports for a pooled connection (`DatabaseMetaData.getURL()`);
- the JDBC URL and driver class HikariCP was configured with, and that HikariCP
  uses that driver rather than a `DataSource` class;
- the class of the dialect Hibernate built;
- that Hibernate takes its connections from that HikariCP pool.

When start-up fails, every test errors; none is skipped. When it succeeds, the
log names the database the run certified:

```text
ConnectionReport : Connected to jdbc:sqlite:.../target/baseline.db?journal_mode=WAL&busy_timeout=5000 with driver org.sqlite.JDBC (SQLite JDBC 3.53.4.0), dialect org.hibernate.community.dialect.SQLiteDialect, database SQLite 3.53.4; this is the baseline stack
```

Ways of pointing the suite elsewhere that were tried on 2026-11-15, and their
results, are listed under "Configuration that was tried" below.

The `flightsql` profile runs the suite through the Apache Arrow Flight SQL JDBC
driver against the server in `internal/spike/flightsql`:

```sh
(cd internal/spike/flightsql && go run . -db /tmp/flightsql.db) &
mvn -f clients/java/acceptance/pom.xml test -Pflightsql
```

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
shares: `ddl-auto=create`, JDBC batch size 50 with ordered inserts, and a
HikariCP pool of up to 4 connections.

### Configuration that was tried

On 2026-11-15, each of these was used on a baseline run to point the suite at
another SQLite file (and, where noted, through another driver class) instead of
`target/baseline.db`. "Rejected" means start-up failed with `the application is
not connected to the requested database` and all 28 tests errored, none
skipped. "Outranked" means Spring read the setting, but a higher-priority
source (the system properties from Surefire) won, so the application stayed on
the requested database, which the log line named, and the run passed as usual.

| Source | Setting | Result |
|---|---|---|
| `SPRING_APPLICATION_JSON` | `spring.datasource.url` | Rejected (pool JDBC URL) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.url` and `driver-class-name`, a wrapper driver on another file | Rejected (pool JDBC URL) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.hikari.jdbc-url` | Rejected (pool JDBC URL) |
| `SPRING_APPLICATION_JSON` | `spring.jpa.properties.hibernate.dialect` | Rejected (Hibernate dialect) |
| `SPRING_APPLICATION_JSON` | `spring.datasource.type` set to Spring's `SimpleDriverDataSource` | Rejected (not HikariCP) |
| `SPRING_APPLICATION_JSON` | Hibernate's own `connection.provider_class` and `connection.url` | Rejected (Hibernate not using the pool) |
| Environment variables | `SPRING_DATASOURCE_URL` and `SPRING_DATASOURCE_HIKARI_JDBC_URL` | Rejected (pool JDBC URL, from the second variable) |
| `JAVA_TOOL_OPTIONS` | `-Dspring.datasource.url=...` | Outranked: Surefire sets the system property again |
| Extra JVM argument (`-Djdbc.argLine`) | `-Dspring.datasource.url=...` | Outranked, as above |
| `spring.config.additional-location` | a file with `spring.datasource.url` | Outranked |
| `spring.config.additional-location` | a file with `spring.datasource.hikari.jdbc-url` | Rejected (pool JDBC URL) |
| Profile-specific file on the class path (`application-evil.properties`, profile activated) | `spring.datasource.url` and `driver-class-name` | Outranked |
| `application.properties` in the working directory | `spring.datasource.hikari.jdbc-url` | Rejected (pool JDBC URL) |
| `config/application.properties` in the working directory | `spring.datasource.hikari.jdbc-url` | Rejected (pool JDBC URL) |

Command-line arguments to the application cannot be passed through Surefire:
the tests start the application themselves.

## What each test stands for

| Test | What it checks | Why an ORM needs it |
|---|---|---|
| `SchemaLifecycleTest.createThenValidate` | Start with `ddl-auto=create`, write a row, stop; start again with `ddl-auto=validate` and read the row back | Hibernate generates DDL from the entities, and `validate` compares the entities with the database's JDBC metadata (`getTables`, `getColumns`); applications commonly validate on start |
| `CrudTest.createReadUpdateDelete` | Insert two rows with `IDENTITY` ids, read, update and delete one | Database-generated ids reach Hibernate through `getGeneratedKeys`, `RETURNING` or an identity select, depending on dialect and driver |
| `RelationsTest.lazyAssociationsLoadInsideTransaction` | `@OneToMany` and lazy `@ManyToOne` are uninitialised after load and load on first access inside a transaction | Lazy loading issues further queries on the same connection and persistence context |
| `RelationsTest.joinFetchLoadsParentAndChildren` | A JPQL `join fetch` loads an author and its books in a fresh persistence context; the collection is initialised, and each book has its id and points to the same author instance | One result row holds columns of two tables, with several identifier columns, which Hibernate reads by alias and assembles into a parent and its children |
| `RelationsTest.cascadeAndOrphanRemovalDeleteBooks` | Removing a book from an author's collection deletes it, and deleting the author deletes its remaining books, checked from a new connection | `CascadeType.ALL` with `orphanRemoval` turns collection changes into `delete` statements |
| `TransactionTest.commitIsVisibleFromSecondConnection` | A `@Transactional` service method saves an author and then each book in separate repository calls; a new connection outside the pool sees all rows | One transaction spans several statements, and a commit must make them all durable together |
| `TransactionTest.exceptionRollsBackEveryWrite` | The same writes, flushed, then an exception; a new connection outside the pool sees no rows | Spring rolls back on a runtime exception; a driver whose `rollback()` does nothing would leave partial writes behind |
| `TransactionTest.rollbackToSavepointKeepsEarlierWrites` | On one pooled connection: insert, `setSavepoint`, insert, `rollback(savepoint)`, commit; only the first row remains | Spring's nested transactions use JDBC savepoints |
| `TransactionTest.failedStatementLeavesTransactionUsable` | Inside a transaction, an insert that breaks a unique constraint fails with `SQLException`; the transaction's other inserts, before and after it, commit | An application may catch a failed statement and carry on in the same transaction; SQLite undoes only the failed statement |
| `QueryTest.derivedQuery` | `findByEmail`, `findByNameStartingWithOrderByNameAsc` | Spring Data turns method names into JPQL with `LIKE` and `ORDER BY` |
| `QueryTest.jpqlQuery` | `@Query` with `size()` on a collection | JPQL is translated to SQL with a correlated subquery |
| `QueryTest.nativeQuery` | `@Query(nativeQuery = true)` with a named parameter | Native SQL goes to the driver as written |
| `QueryTest.paginationAndSorting` | `findAll(PageRequest.of(1, 3, Sort...))` | Paging issues `LIMIT`/`OFFSET` from the dialect plus a `count` query |
| `QueryTest.bulkUpdateReportsAffectedRows` | A JPQL bulk update returns 1 for an existing row and 0 for a missing one | `@Modifying` queries return the driver's update count from `executeUpdate`; Hibernate's own entity updates go through `executeBatch` instead |
| `QueryTest.duplicateColumnLabelReadsTheFirstColumn` | In a join whose two columns share the label `name`, `findColumn("name")` is 1 and `getString("name")` is the first column's value | JDBC returns the first matching column for a label ([`java.sql.ResultSet`](https://docs.oracle.com/en/java/javase/25/docs/api/java.sql/java/sql/ResultSet.html)); code that reads by label relies on it |
| `OptimisticLockingTest.versionIncrementsAndStaleUpdateFails` | `@Version` increments, and saving a stale detached copy raises `ObjectOptimisticLockingFailureException` | Saving a detached copy goes through Hibernate's merge, which loads the current row and rejects the version before any `UPDATE` is sent; this test does not reach the driver's update count |
| `OptimisticLockingTest.updateMatchingNoRowFailsOnFlush` | Inside a transaction, a managed account is loaded, its stored version is advanced by plain JDBC on the same connection, and the entity is changed and flushed; the flush raises `ObjectOptimisticLockingFailureException` and the row keeps its balance | The flush sends `update ... where id = ? and version = ?`, which matches no row; only the update count of 0 from the driver tells Hibernate the update failed. A driver that reports 1 for every update fails this test |
| `BatchInsertTest.insertsAreBatched` | 500 inserts with JDBC batching on; Hibernate prepares the insert at most once per batch of 50; read back through a fresh persistence context, the sequence values are exactly 0 to 499, once each, with distinct ids, and in `rowid` order they are in the order they were added to the batch | Batching uses `addBatch`/`executeBatch` on a prepared statement; ids are application-generated UUIDs because Hibernate does not batch `IDENTITY` inserts |
| `ColumnTypesTest.valuesRoundTrip` | String (non-ASCII), long, double, boolean, `BigDecimal`, `Instant`, `LocalDate` and a 300 KiB `byte[]` in a `blob` column round-trip; a second row with `false`, an empty string and a different 3-byte payload round-trips too | Each Java type goes through a dialect type mapping and a specific `ResultSet` getter |
| `ColumnTypesTest.nullsRoundTrip` | The same columns, all null | Binding null needs the right SQL type from the dialect |
| `LobTest.lobRoundTrips` | A 300 KiB `@Lob byte[]` round-trips | With the community dialect, Hibernate reads this mapping through `ResultSet.getBlob`; see "Known baseline failures" |
| `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation` | A duplicate unique value raises Spring's `DataIntegrityViolationException` | Applications catch this exception; Hibernate has to recognise the driver's error as a constraint violation |
| `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation` | Saving a book whose author id does not exist raises `DataIntegrityViolationException`, and no book is stored | The schema Hibernate generates must declare the foreign key, and the database must enforce it |
| `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation` | A JPQL update that sets a `nullable = false` column to null raises `DataIntegrityViolationException`, and the row is unchanged | Hibernate checks nulls itself before an insert, but not in a JPQL update, so here the database's `not null` constraint and the error conversion are tested |
| `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` | Two HikariCP connections held at once; an uncommitted insert on one is not visible on the other, and rollback removes it | A pool shares one database between threads, which relies on transaction isolation |
| `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection` | Two pooled connections held open: the reader, in autocommit, reads 0; the writer inserts and calls `commit()`; the reader then reads 1, before either connection is closed or has its autocommit reset | Closing a connection or resetting its autocommit also commits, so tests that check from a new connection cannot tell a working `commit()` from one that does nothing; a reader that keeps returning its first result is caught too |
| `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` | Two pooled connections each insert, one after the other; each statement's `getGeneratedKeys` returns the id of its own row | Identity ids reach Hibernate through the connection that inserted; a driver that asks the server for "the last inserted id" outside that connection returns another thread's id |
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
that with no fault all 28 tests run and pass. Then it injects the one fault
named by `-Dacceptance.fault`. `FaultCheck` runs the whole suite in a separate
JVM once with no fault, which must pass every test with none skipped, and once
per fault, which must fail at least one test and must have applied the fault
at least once. The `java-acceptance` CI job runs it after the baseline; it
took 242 seconds on an 8-core machine. `-Dacceptance.faults=a,b` runs a
subset.

The table below is from the run on 2026-11-15 at commit 9756b83. "Applied" is
how many times the fault changed a call or its result; each run has its own
database file.

| Fault | What the wrapper does | Applied | Tests failed | Caught by |
|---|---|---|---|---|
| none (control) | Repairs only | 0 | 0 of 28 | |
| `commit-noop` | `commit()` returns without committing | 263 | 1 | `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection` |
| `rollback-noop` | `rollback()` returns without rolling back | 7 | 2 | `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection`, `TransactionTest.exceptionRollsBackEveryWrite` |
| `autocommit-ignored` | `setAutoCommit(false)` is ignored, `getAutoCommit()` reports what was asked, `commit()` and `rollback()` do nothing | 539 | 2 | `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection`, `TransactionTest.exceptionRollsBackEveryWrite` |
| `stale-read` | On a connection in autocommit, a repeated query with the same parameters returns the values it returned the first time | 1 | 1 | `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection` |
| `dirty-read` | Connections share SQLite's cache with `read_uncommitted`, so one reads another's uncommitted rows | 19 | 1 | `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` |
| `update-count-one` | `executeUpdate` and `getUpdateCount` return 1 | 158 | 1 | `QueryTest.bulkUpdateReportsAffectedRows` |
| `update-count-zero` | `executeUpdate` and `getUpdateCount` return 0 | 143 | 4 | `QueryTest.bulkUpdateReportsAffectedRows` and three `ConnectionPoolTest` tests |
| `batch-count-one` | Every count from `executeBatch` is 1 | 1 | 1 | `OptimisticLockingTest.updateMatchingNoRowFailsOnFlush` |
| `batch-drop-row` | The first row of each batch is not executed; the counts still cover every row | 10 | 1 | `BatchInsertTest.insertsAreBatched` |
| `batch-wrong-values` | In a batch, every `setInt` value is bound as 0 | 499 | 1 | `BatchInsertTest.insertsAreBatched` |
| `batch-reordered` | The rows of each batch are executed in reverse order | 10 | 1 | `BatchInsertTest.insertsAreBatched` |
| `generated-key-wrong` | Generated keys, and results of inserts and `last_insert_rowid()`, are 1000 too high | 30 | 19 | 19 tests, including `CrudTest.createReadUpdateDelete` and `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` |
| `generated-key-other-connection` | `getGeneratedKeys` returns the keys of the most recent insert on any connection | 1 | 1 | `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` |
| `long-truncated` | `getLong` truncates to an `int` | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `double-rounded` | `getDouble` rounds to `float` precision | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `boolean-inverted` | `getBoolean` returns the opposite | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `decimal-scale-lost` | `getBigDecimal` rounds to scale 0 | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `decimal-precision-lost` | `getBigDecimal` rounds to 6 significant digits | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `instant-shifted` | `getTimestamp` and `getObject` for date-times add two hours | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `date-off-by-one` | `getDate` and `getObject` for dates return the day before | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `string-non-ascii-corrupted` | `getString` replaces non-ASCII characters with `?` | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `empty-string-as-null` | `getString` returns null for an empty string, and `wasNull()` agrees | 1 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `null-as-empty-string` | `getString` returns an empty string for null, and `wasNull()` agrees | 1 | 1 | `ColumnTypesTest.nullsRoundTrip` |
| `blob-altered` | `getBytes` and `getBlob` flip one bit | 502 | 2 | `ColumnTypesTest.valuesRoundTrip`, `LobTest.lobRoundTrips` |
| `blob-truncated` | `getBytes` and `getBlob` drop the last byte | 502 | 2 | `ColumnTypesTest.valuesRoundTrip`, `LobTest.lobRoundTrips` |
| `blob-wrong-row` | `getBytes` and `getBlob` return the bytes read before, from another row | 501 | 1 | `ColumnTypesTest.valuesRoundTrip` |
| `params-swapped` | Parameters 1 and 2 of a prepared statement are bound to each other's position | 1094 | 22 | 22 tests, including `CrudTest.createReadUpdateDelete` and `BatchInsertTest.insertsAreBatched` |
| `null-param-as-zero-or-empty` | `setNull` and a null `setObject` bind `''` for character types, empty bytes for binary types and 0 otherwise | 14 | 2 | `ColumnTypesTest.nullsRoundTrip`, `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation` |
| `limit-ignored` | `limit ?` becomes no limit | 1 | 1 | `QueryTest.paginationAndSorting` |
| `offset-ignored` | `offset ?` becomes offset 0 | 1 | 1 | `QueryTest.paginationAndSorting` |
| `order-by-ignored` | `order by` is removed from queries, so rows come in storage order | 5 | 3 | `QueryTest.derivedQuery`, `QueryTest.jpqlQuery`, `QueryTest.paginationAndSorting` |
| `result-stops-early` | `next()` returns false after 100 rows | 1 | 1 | `BatchInsertTest.insertsAreBatched` |
| `duplicate-rows` | Each result set returns its first row twice | 127 | 22 | 22 tests, including `BatchInsertTest.insertsAreBatched` and `ColumnTypesTest.valuesRoundTrip` |
| `label-last-column` | `findColumn` and getters by label use the last column with that label, not the first | 1 | 1 | `QueryTest.duplicateColumnLabelReadsTheFirstColumn` |
| `constraint-wrong-state` | A constraint error is a plain `SQLException` with SQL state 42000 (syntax error) | 4 | 3 | the three `ConstraintTest` tests |
| `error-swallowed` | A failing `execute`, `executeUpdate` or `executeBatch` reports success (count 1) | 4 | 4 | the three `ConstraintTest` tests, `TransactionTest.failedStatementLeavesTransactionUsable` |
| `product-name-wrong` | `getDatabaseProductName()` returns `PostgreSQL` | 13 | 1 | `MetadataTest.productNameAndVersionMatchTheEngine` |
| `product-version-wrong` | `getDatabaseProductVersion()` returns `3.8.0` and `getDatabaseMinorVersion()` 8 | 25 | 1 | `MetadataTest.productNameAndVersionMatchTheEngine` |

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
- Each fault is injected alone. Combinations were not tried.

## Known baseline failures

Five tests fail on the baseline (Spring Boot 4.1.2, Hibernate ORM 7.4.13,
sqlite-jdbc 3.53.4.0). They show requirements on a driver and dialect together,
not defects of one side: for each one, the notes below say which side could
satisfy it. Those notes were read from the pinned source and were not tested,
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
  Either side could satisfy it: a dialect that maps code 19, or a driver that
  raises `SQLIntegrityConstraintViolationException`
  ([SQLExceptionTypeDelegate.java#L55-L60](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLExceptionTypeDelegate.java#L55-L60))
  or a class 23 SQL state
  ([SQLStateConversionDelegate.java#L64-L77](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLStateConversionDelegate.java#L64-L77)),
  with the community dialect unchanged. Either way Hibernate throws its
  `ConstraintViolationException`, which Spring turns into
  `DataIntegrityViolationException`
  ([HibernateExceptionTranslator.java#L165-L168](https://github.com/spring-projects/spring-framework/blob/v7.0.10/spring-orm/src/main/java/org/springframework/orm/jpa/hibernate/HibernateExceptionTranslator.java#L165-L168)).
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
  and a driver or server that turns enforcement on for every connection.

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
- `LocalDateTime` and `OffsetDateTime` columns, enums, embeddables,
  `@ElementCollection`, inheritance, many-to-many associations;
- pessimistic locking, isolation levels other than the default, and
  concurrency beyond two connections held by one thread;
- connection loss and reconnection, TLS, authentication and performance.
