---
title: "0003: JDBC transport"
description: skylite will ship its own JDBC driver over its HTTPS API, because a Spring Data JPA acceptance suite did not pass through the Arrow Flight SQL JDBC driver.
weight: 3
---

**Status:** Accepted

**Date:** 2026-11-15

## Context

skylite's requirements include a JDBC driver and a Hibernate dialect that work with Spring Data JPA, for Java services that reach skylite over HTTPS through an Istio mesh or an ingress, with no raw TCP. The [architecture page](/docs/architecture/#wire-protocol-for-jdbc) left open what that driver talks to. Two candidates were considered:

- **Candidate A:** skylite speaks Arrow Flight SQL over gRPC, and Java services use the existing Apache Arrow Flight SQL JDBC driver with Hibernate's community SQLite dialect. skylite would write no Java code.
- **Candidate B:** skylite ships its own thin JDBC driver over its HTTPS API.

The project fixed the rule before testing: candidate A is chosen only if a realistic Spring Data JPA application passes its whole acceptance suite on A with no workarounds in application code. Otherwise candidate B.

## Method

The pull request that adds this record also adds four things, all of which run without skylite, because no skylite server exists yet:

- **An acceptance suite** in `clients/java/acceptance/`: a Spring Boot application with Spring Data JPA, Hibernate and HikariCP, and 28 tests covering schema creation and validation, CRUD with database-generated ids, lazy one-to-many and many-to-one associations, a JPQL `join fetch`, cascade and orphan removal, `@Transactional` commit and rollback checked from a second connection, a commit seen by a connection that is already open, savepoints, a failed statement inside a transaction, derived, JPQL and native queries, pagination and sorting, bulk update counts, column lookup by a duplicated label, optimistic locking on a detached copy and on a managed entity whose update matches no row, batch inserts read back in full, column types, `false`, the empty string and nulls, a `@Lob`, unique, foreign-key and not-null constraint violations, isolation between pooled connections, generated keys on two connections, and the product name and version in `DatabaseMetaData`. Its README says what each test stands for. The JDBC URL, driver class and dialect come from configuration, and the suite fails start-up unless the connection it actually uses matches them.
- **A baseline run** of that suite through the reference SQLite JDBC driver on a temporary file database, to show that the suite and the dialect work, so that a failure on candidate A belongs to A. CI runs it on every pull request as the `java-acceptance` job.
- **A check of the suite itself**: a wrapper JDBC driver around sqlite-jdbc that repairs the five baseline failures and then injects one driver fault at a time, such as a `commit()` that does nothing, a stale read, a batch that binds wrong values or a long truncated to an int. The suite runs once with no fault, where every test must pass with none skipped, and once per fault, where at least one test must fail. All 38 faults were caught. The README lists them and the tests that caught each; the `java-acceptance` CI job runs this check as well. It shows which faults the suite is known to catch, not that a driver that passes it is correct.
- **A Flight SQL server** in `internal/spike/flightsql/`, a nested Go module that serves the SQLite example server from the Apache Arrow Go module on a SQLite file in WAL mode. The suite ran against it through the Arrow Flight SQL JDBC driver.

Versions:

| Component | Version | Source |
|---|---|---|
| Spring Boot | 4.1.2 | `clients/java/acceptance/pom.xml` |
| Hibernate ORM and community dialects | 7.4.13.Final | managed by Spring Boot 4.1.2 ([build.gradle](https://github.com/spring-projects/spring-boot/blob/v4.1.2/platform/spring-boot-dependencies/build.gradle#L663)) |
| HikariCP | 7.0.2 | managed by Spring Boot 4.1.2 ([build.gradle](https://github.com/spring-projects/spring-boot/blob/v4.1.2/platform/spring-boot-dependencies/build.gradle#L701)) |
| sqlite-jdbc (baseline) | 3.53.4.0 | managed by Spring Boot 4.1.2 ([build.gradle](https://github.com/spring-projects/spring-boot/blob/v4.1.2/platform/spring-boot-dependencies/build.gradle#L2983)) |
| Arrow Flight SQL JDBC driver (candidate A) | 19.0.0, which bundles Avatica 1.26.0 ([pom.xml](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/pom.xml#L158-L162)) | `clients/java/acceptance/pom.xml` |
| Arrow Go, SQLite example server | v18.8.0 | `internal/spike/flightsql/go.mod` |
| JDK | Temurin 25 | local run and the CI job |

The five tests that fail on the baseline are skipped only when the driver is sqlite-jdbc and the dialect is the community SQLite dialect, so the candidate A run included them with no extra flag. The suite's README gives the commands.

To keep the Arrow driver's allocator from failing at start-up on Java 25, the run passed `--sun-misc-unsafe-memory-access=allow` as well as the `--add-opens` flag the driver documents ([flight_sql_jdbc_driver.rst#L30-L35](https://github.com/apache/arrow-java/blob/v19.0.0/docs/source/flight_sql_jdbc_driver.rst#L30-L35)). Without it, the copy of Netty inside the driver disables `sun.misc.Unsafe` on Java 25 and later ([PlatformDependent0.java#L554-L563](https://github.com/netty/netty/blob/netty-4.2.9.Final/common/src/main/java/io/netty/util/internal/PlatformDependent0.java#L554-L563); version 4.2.9.Final per the `META-INF/DEPENDENCIES` file in the driver jar), and every test fails with `ExceptionInInitializerError` in `RootAllocator`. These are JVM options, not application code.

## Results

Three kinds of failure on candidate A are distinguished:

- **Driver:** the JDBC driver cannot do it at all.
- **Server:** the example server does not implement it, but a skylite server could.
- **Dialect and driver:** Hibernate and the driver disagree, for example on metadata, generated keys or type mapping.

Only driver and dialect-and-driver failures count against candidate A.

| Test | Baseline | Candidate A | Cause on A (first failure) | Kind |
|---|---|---|---|---|
| `SchemaLifecycleTest.createThenValidate` | Fail (known, skipped) | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `CrudTest.createReadUpdateDelete` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `RelationsTest.lazyAssociationsLoadInsideTransaction` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `RelationsTest.joinFetchLoadsParentAndChildren` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `RelationsTest.cascadeAndOrphanRemovalDeleteBooks` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `TransactionTest.commitIsVisibleFromSecondConnection` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `TransactionTest.exceptionRollsBackEveryWrite` | Pass | Fail | [F1](#f1-generated-identifiers); [F3](#f3-transactions) would follow | Dialect and driver; driver |
| `QueryTest.derivedQuery` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `QueryTest.jpqlQuery` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `QueryTest.nativeQuery` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `TransactionTest.rollbackToSavepointKeepsEarlierWrites` | Pass | Fail | [F2](#f2-parameter-and-result-schemas) | Server |
| `TransactionTest.failedStatementLeavesTransactionUsable` | Pass | Fail | [F2](#f2-parameter-and-result-schemas) | Server |
| `QueryTest.paginationAndSorting` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `QueryTest.bulkUpdateReportsAffectedRows` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `QueryTest.duplicateColumnLabelReadsTheFirstColumn` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `OptimisticLockingTest.versionIncrementsAndStaleUpdateFails` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `OptimisticLockingTest.updateMatchingNoRowFailsOnFlush` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `BatchInsertTest.insertsAreBatched` | Pass | Fail | [F2](#f2-parameter-and-result-schemas) | Server |
| `ColumnTypesTest.valuesRoundTrip` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `ColumnTypesTest.nullsRoundTrip` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `LobTest.lobRoundTrips` | Fail (known, skipped) | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation` | Fail (known, skipped) | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation` | Fail (known, skipped) | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation` | Fail (known, skipped) | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` | Pass | Fail | [F2](#f2-parameter-and-result-schemas); [F3](#f3-transactions) would follow | Server; driver |
| `ConnectionPoolTest.commitIsVisibleToAnOpenPooledConnection` | Pass | Fail | `NullPointerException` from the driver's result set on the prepared count query; inferred to be F2's missing result schema, not traced | Server (inferred) |
| `ConnectionPoolTest.generatedKeyBelongsToTheStatementsOwnInsert` | Pass | Fail | [F1](#f1-generated-identifiers): the test calls `prepareStatement(sql, RETURN_GENERATED_KEYS)` itself, and the driver throws `SQLFeatureNotSupportedException` | Driver |
| `MetadataTest.productNameAndVersionMatchTheEngine` | Pass | Fail | [F2](#f2-parameter-and-result-schemas): `select sqlite_version()` failed with `Statement did not return a result set`. The driver also reported the product name `db_name` and version `sqlite 3`, which the test would then have rejected | Server |

Baseline: 23 passed and 5 failed when every test was run; by default, as in the CI job, those 5 are skipped, so 23 pass and 5 are skipped. Candidate A: 0 of 28 passed (26 errors and 2 assertion failures; the 2 assertion failures, in `TransactionTest.exceptionRollsBackEveryWrite` and `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation`, are the F1 exception arriving where the test expected another). Each test stops at its first failure, so the table names the first cause, and the steps after it did not run. Where a later step depends on transactions, the table adds [F3](#f3-transactions), which was shown separately.

Hibernate's `ddl-auto=create` did run on candidate A: the tables were created, and the tests then failed on their first insert or parameter bind.

### Baseline failures

Five tests fail on the baseline. They are requirements on skylite's driver and dialect together, not defects that a skylite dialect would simply inherit from the community one: for some of them the driver alone could satisfy the requirement. The suite's README traces each failure and links the source. In summary, read from the pinned source and not tested:

- Schema validation: the dialect creates `IDENTITY` columns as `integer`, and validation compares the entity's `bigint` with the type the driver reports from `DatabaseMetaData.getColumns` through the dialect's `equivalentTypes` ([ColumnDefinitions.java#L28-L30](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/tool/schema/internal/ColumnDefinitions.java#L28-L30)). Either side could satisfy it: the dialect by treating the two as equivalent, or the driver by reporting the column as `BIGINT`.
- `@Lob byte[]`: with the community dialect, Hibernate reads this mapping with `ResultSet.getBlob` ([SQLiteDialect.java#L367](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L367), [BlobJdbcType.java#L58-L62](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BlobJdbcType.java#L58-L62)), which sqlite-jdbc does not implement. A driver that implements `getBlob` would satisfy it with the community dialect unchanged; so would a dialect that registers the materialized BLOB descriptor, which reads with `getBytes` ([BlobJdbcType.java#L256-L262](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BlobJdbcType.java#L256-L262)).
- Unique and not-null violations: SQLite rejects the write, but sqlite-jdbc raises a plain `SQLException` with no SQL state, the dialect does not map its error code ([SQLiteDialect.java#L451-L470](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L451-L470)), and Spring raises `JpaSystemException` instead of `DataIntegrityViolationException`. A driver that raises `SQLIntegrityConstraintViolationException` ([SQLExceptionTypeDelegate.java#L55-L60](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLExceptionTypeDelegate.java#L55-L60)) or a class 23 SQL state ([SQLStateConversionDelegate.java#L64-L77](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/exception/internal/SQLStateConversionDelegate.java#L64-L77)) would get Hibernate's standard conversion with no dialect change; so would a dialect that maps the code.
- Foreign keys: the generated `book` table has no foreign key, because the community dialect reports no `ALTER TABLE` support and Hibernate then emits none ([StandardForeignKeyExporter.java#L37-L42](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/tool/schema/internal/StandardForeignKeyExporter.java#L37-L42)), and `PRAGMA foreign_keys` was 0 on the suite's connections, so SQLite would not enforce one either; both were checked on the baseline database. This one needs both sides: the dialect has to declare the constraint and the connection has to enforce it.

One limit was also found: `Instant` values kept milliseconds only, so the suite tests at millisecond precision.

### F1: generated identifiers

Every insert of an entity with an `IDENTITY` id failed with `SQLFeatureNotSupportedException` from `AvaticaConnection.prepareStatement`. The driver reports `supportsGetGeneratedKeys() == false`, and Hibernate accordingly uses its selecting delegate, which prepares the insert with `prepareStatement(sql, Statement.NO_GENERATED_KEYS)` ([AbstractSelectingDelegate.java#L60-L64](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/id/insert/AbstractSelectingDelegate.java#L60-L64)). Avatica, which the Arrow driver is built on, throws for that overload whatever the flag value ([AvaticaConnection.java#L373-L376](https://github.com/apache/calcite-avatica/blob/rel/avatica-1.26.0/core/src/main/java/org/apache/calcite/avatica/AvaticaConnection.java#L373-L376)). Hibernate and the driver disagree on a standard JDBC call, so this is a dialect-and-driver failure, and the server cannot change it.

What was and was not tried against F1:

- `hibernate.jdbc.use_get_generated_keys=false` was not run on its own. Hibernate defaults that setting to the driver's `supportsGetGeneratedKeys()` ([SessionFactoryOptionsBuilder.java#L505-L506](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/boot/internal/SessionFactoryOptionsBuilder.java#L505-L506)), which is `false` here, so setting it to `false` keeps the selecting delegate that failed ([IdentityGenerator.java#L66-L92](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/id/IdentityGenerator.java#L66-L92)).
- `hibernate.jdbc.use_get_generated_keys=true` was run with `CrudTest`. Hibernate then chose its generated-keys delegate, which prepares the insert with `prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)` or `prepareStatement(sql, String[])` ([GetGeneratedKeysDelegate.java#L77-L83](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/id/insert/GetGeneratedKeysDelegate.java#L77-L83)); the test failed with `SQLFeatureNotSupportedException` from that call, and Avatica rejects the `String[]` and `int[]` overloads as well ([AvaticaConnection.java#L373-L386](https://github.com/apache/calcite-avatica/blob/rel/avatica-1.26.0/core/src/main/java/org/apache/calcite/avatica/AvaticaConnection.java#L373-L386)).
- Hibernate's insert-returning delegate, which a dialect can select by declaring `insert ... returning` support (the community SQLite dialect does not), prepares its statement with `prepareStatement(sql, NO_GENERATED_KEYS)` as well ([InsertReturningDelegate.java#L93-L96](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/id/insert/InsertReturningDelegate.java#L93-L96)), so, read from source and not tested, it would meet the same rejection.
- Changing the entities' identifier strategy, for example to application-generated UUIDs, would avoid F1 but is a change to application code, which the rule excludes.
- A custom dialect or a custom generated-values delegate that inserts through an overload Avatica accepts was not evaluated. Whether one could avoid F1 is open.

None of this changes the outcome: F3 alone fails candidate A under the rule.

Inferred, not tested: if that call were accepted, the selecting delegate would next run `select last_insert_rowid()` ([SQLiteIdentityColumnSupport.java#L35-L37](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/identity/SQLiteIdentityColumnSupport.java#L35-L37)) as a separate statement, and the example server runs statements on a Go `database/sql` pool ([sqlite_server.go#L415-L426](https://github.com/apache/arrow-go/blob/v18.8.0/arrow/flight/flightsql/example/sqlite_server.go#L415-L426)), which may give it a different SQLite connection from the insert.

### F2: parameter and result schemas

`BatchInsertTest` and `ConnectionPoolTest` do not use `IDENTITY` inserts, and failed on their first parameter bind with `parameter ordinal 1 out of range`. The driver takes the number and types of parameters from the parameter schema the server returns when a statement is prepared ([ArrowFlightMetaImpl.java#L176-L183](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L176-L183), [AvaticaParameterBinder.java#L94-L98](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/utils/AvaticaParameterBinder.java#L94-L98)). The example server returns neither a parameter schema nor a result schema ([sqlite_server.go#L433-L437](https://github.com/apache/arrow-go/blob/v18.8.0/arrow/flight/flightsql/example/sqlite_server.go#L433-L437)).

A separate probe showed the same gap for result sets: through this server, `Statement.executeQuery("select count(*) ...")` failed with `Statement did not return a result set`, because the driver treats a statement with an empty result schema as an update ([ArrowFlightMetaImpl.java#L65-L68](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L65-L68)).

Both belong to the example server: a server that returns the schemas would avoid them. That a skylite server could return correct schemas for every statement before running it is an inference and was not tested.

### F3: transactions

In the driver, `commit` and `rollback` are empty stubs ([ArrowFlightMetaImpl.java#L90-L93](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L90-L93), [ArrowFlightMetaImpl.java#L245-L248](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L245-L248)), and statements are prepared without a transaction ([ArrowFlightSqlClientHandler.java#L443-L445](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/client/ArrowFlightSqlClientHandler.java#L443-L445)), although the Flight SQL client library it uses has a transactional `prepare` and `beginTransaction` ([FlightSqlClient.java#L891](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql/src/main/java/org/apache/arrow/flight/sql/FlightSqlClient.java#L891), [FlightSqlClient.java#L944](https://github.com/apache/arrow-java/blob/v19.0.0/flight/flight-sql/src/main/java/org/apache/arrow/flight/sql/FlightSqlClient.java#L944)). The example server does implement transactions ([sqlite_server.go#L783-L800](https://github.com/apache/arrow-go/blob/v18.8.0/arrow/flight/flightsql/example/sqlite_server.go#L783-L800)) and advertises them ([sqlite_info.go#L42](https://github.com/apache/arrow-go/blob/v18.8.0/arrow/flight/flightsql/example/sqlite_info.go#L42)).

The probe in the [appendix](#appendix-the-rollback-probe) uses plain JDBC, with no Hibernate, Spring or pool. It turned autocommit off, inserted a row and called `rollback()`, which returned without error; the row was still in the SQLite file afterwards. This is a driver failure: no server can make `rollback()` undo a write when the driver never sends a transaction. It means a Spring `@Transactional` method that throws would leave its earlier writes in place, and an uncommitted write would be visible to every other connection.

### Through an HTTP/2 proxy

Not run. The rule called for an Envoy run only if the suite passed on candidate A, and it did not.

## Decision

The rule produces candidate B: skylite will ship its own JDBC driver over its HTTPS API. Candidate A failed 28 of 28 tests, including failures of the driver kind (transactions, F3) and of the dialect-and-driver kind (generated identifiers, F1), which count against it under the rule. F3 is decisive on its own: `commit` and `rollback` are empty in the driver itself, so, as read from the source, a server, a Hibernate setting or a dialect has no call to act on.

## Alternatives considered

- **Candidate A, Arrow Flight SQL with the Arrow JDBC driver.** Rejected by the rule above. Transactions failing in the driver was the deciding result; a skylite Flight SQL server could not fix F1 or F3. For F1, `use_get_generated_keys` was checked both ways (`true` by a run, `false` from source), and a custom dialect or generated-values delegate was not evaluated ([F1](#f1-generated-identifiers)); none of these reaches F3.
- **Candidate A with a patched or forked Arrow driver.** Not tested. It would mean maintaining Java driver code anyway, which is what candidate A was meant to avoid, and the rule tested the driver as published.

## Consequences

- skylite has to write and maintain a JDBC driver that talks to its HTTPS API, and a Hibernate dialect. The suite in `clients/java/acceptance/` is the yardstick for both, and will run against skylite's endpoint once one exists.
- The five baseline failures are requirements on skylite's driver and dialect together: schema validation, `@Lob` and unique and not-null violations can be satisfied by either side, and foreign keys need both (see [Baseline failures](#baseline-failures)). Which side takes each one is for the driver's design.
- The `java-acceptance` CI job runs the baseline on every pull request, merge-queue group and push to `main`. It is not a required check until the ruleset on `main` is changed to include it.
- `internal/spike/flightsql/` is a nested module kept to reproduce candidate A's result. `go build ./...` at the repository root does not include it.
- The [question](/docs/architecture/#wire-protocol-for-jdbc) on the architecture page is updated in the same pull request to state this decision.

## Limits

- One driver release (Arrow Flight SQL JDBC 19.0.0), one server (the Arrow Go v18.8.0 SQLite example), one JDK (Temurin 25) and one machine (Linux, local) were tested. In arrow-java's `main` branch at commit `a62942c0`, `commit` ([ArrowFlightMetaImpl.java#L57-L60](https://github.com/apache/arrow-java/blob/a62942c0c8ab73e7a86236c8d19321c7aebe07ff/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L57-L60)) and `rollback` ([ArrowFlightMetaImpl.java#L158-L161](https://github.com/apache/arrow-java/blob/a62942c0c8ab73e7a86236c8d19321c7aebe07ff/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L158-L161)) were still stubs on 2026-11-15. v19.0.0 was the latest release then.
- The spike server opens a SQLite file in WAL mode instead of the example's in-memory database with sample tables; everything else is the example server unchanged.
- Each test stopped at its first failure, so most later steps on candidate A were not reached. Results beyond the first failure come from the source and the separate probes, not from the suite.
- No server that returns parameter and result schemas was built, so how candidate A would fare against a more complete Flight SQL server is untested; F1 and F3 do not depend on the server.
- No HTTP/2 proxy, TLS, authentication or performance was tested.
- The baseline covers the suite's 28 tests only. It does not show that sqlite-jdbc or the community dialect work for features the suite does not test; the suite's README lists routine behaviour it does not cover.
- The fault check shows that the suite catches the 38 faults it injects, each in isolation, on SQLite. It does not show that a driver that passes the suite is correct.

## Appendix: the rollback probe

Run with the driver jar from Maven Central on the class path and the spike server listening on port 31337, then count the rows in the `probe` table of the server's SQLite file with any SQLite client. The count was 1.

```java
import java.sql.*;

public class Rollback {
    public static void main(String[] a) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:arrow-flight-sql://127.0.0.1:31337?useEncryption=false");
                Statement s = c.createStatement()) {
            s.executeUpdate("create table probe (id integer primary key, v text)");
            c.setAutoCommit(false);
            s.executeUpdate("insert into probe (v) values ('uncommitted')");
            c.rollback();
            System.out.println("rollback() returned without error");
        }
    }
}
```
