---
title: "0003: JDBC transport"
description: skylite will ship its own JDBC driver over its HTTPS API, because a Spring Data JPA acceptance suite did not pass through the Arrow Flight SQL JDBC driver.
weight: 3
---

**Status:** Proposed

**Date:** 2026-11-15

## Context

skylite's requirements include a JDBC driver and a Hibernate dialect that work with Spring Data JPA, for Java services that reach skylite over HTTPS through an Istio mesh or an ingress, with no raw TCP. The [architecture page](/docs/architecture/#wire-protocol-for-jdbc) left open what that driver talks to. Two candidates were considered:

- **Candidate A:** skylite speaks Arrow Flight SQL over gRPC, and Java services use the existing Apache Arrow Flight SQL JDBC driver with Hibernate's community SQLite dialect. skylite would write no Java code.
- **Candidate B:** skylite ships its own thin JDBC driver over its HTTPS API.

The project fixed the rule before testing: candidate A is chosen only if a realistic Spring Data JPA application passes its whole acceptance suite on A with no workarounds in application code. Otherwise candidate B.

## Method

The pull request that adds this record also adds three things, all of which run without skylite, because no skylite server exists yet:

- **An acceptance suite** in `clients/java/acceptance/`: a Spring Boot application with Spring Data JPA, Hibernate and HikariCP, and 16 tests covering schema creation and validation, CRUD with database-generated ids, lazy one-to-many and many-to-one associations, `@Transactional` commit and rollback checked from a second connection, derived, JPQL and native queries, pagination and sorting, optimistic locking, batch inserts, column types and nulls, a `@Lob`, a unique-constraint violation, and isolation between pooled connections. Its README says what each test stands for. The JDBC URL, driver class and dialect come from configuration.
- **A baseline run** of that suite through the reference SQLite JDBC driver on a temporary file database, to show that the suite and the dialect work, so that a failure on candidate A belongs to A. CI runs it on every pull request as the `java-acceptance` job.
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

The candidate A run used `-Djunit.jupiter.conditions.deactivate='org.junit.*DisabledCondition'`, so it also ran the three tests that are disabled because they fail on the baseline. The suite's README gives the commands.

To keep the Arrow driver's allocator from failing at start-up on Java 25, the run passed `--sun-misc-unsafe-memory-access=allow` as well as the `--add-opens` flag the driver documents ([flight_sql_jdbc_driver.rst#L30-L35](https://github.com/apache/arrow-java/blob/v19.0.0/docs/source/flight_sql_jdbc_driver.rst#L30-L35)). Without it, the copy of Netty inside the driver disables `sun.misc.Unsafe` on Java 25 and later ([PlatformDependent0.java#L554-L563](https://github.com/netty/netty/blob/netty-4.2.9.Final/common/src/main/java/io/netty/util/internal/PlatformDependent0.java#L554-L563); version 4.2.9.Final per the `META-INF/DEPENDENCIES` file in the driver jar), and every test fails with `ExceptionInInitializerError` in `RootAllocator`. These are JVM options, not application code.

## Results

Three kinds of failure on candidate A are distinguished:

- **Driver:** the JDBC driver cannot do it at all.
- **Server:** the example server does not implement it, but a skylite server could.
- **Dialect and driver:** Hibernate and the driver disagree, for example on metadata, generated keys or type mapping.

Only driver and dialect-and-driver failures count against candidate A.

| Test | Baseline | Candidate A | Cause on A (first failure) | Kind |
|---|---|---|---|---|
| `SchemaLifecycleTest.createThenValidate` | Fail (known, disabled) | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `CrudTest.createReadUpdateDelete` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `RelationsTest.lazyAssociationsLoadInsideTransaction` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `TransactionTest.commitIsVisibleFromSecondConnection` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `TransactionTest.exceptionRollsBackEveryWrite` | Pass | Fail | [F1](#f1-generated-identifiers); [F3](#f3-transactions) would follow | Dialect and driver; driver |
| `QueryTest.derivedQuery` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `QueryTest.jpqlQuery` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `QueryTest.nativeQuery` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `QueryTest.paginationAndSorting` | Pass | Fail | [F1](#f1-generated-identifiers), while inserting test data | Dialect and driver |
| `OptimisticLockingTest.versionIncrementsAndStaleUpdateFails` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `BatchInsertTest.insertsAreBatched` | Pass | Fail | [F2](#f2-parameter-and-result-schemas) | Server |
| `ColumnTypesTest.valuesRoundTrip` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `ColumnTypesTest.nullsRoundTrip` | Pass | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `LobTest.lobRoundTrips` | Fail (known, disabled) | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation` | Fail (known, disabled) | Fail | [F1](#f1-generated-identifiers) | Dialect and driver |
| `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` | Pass | Fail | [F2](#f2-parameter-and-result-schemas); [F3](#f3-transactions) would follow | Server; driver |

Baseline: 13 passed and 3 failed for reasons in the dialect or sqlite-jdbc; the CI job runs those 3 as skipped. Candidate A: 0 of 16 passed. Each test stops at its first failure, so the table names the first cause, and the steps after it did not run. Where a later step depends on transactions, the table adds [F3](#f3-transactions), which was shown separately.

Hibernate's `ddl-auto=create` did run on candidate A: the tables were created, and the tests then failed on their first insert or parameter bind.

### Baseline failures

The three baseline failures come from the Hibernate community SQLite dialect and sqlite-jdbc. The suite's README traces each one:

- Schema validation: the dialect creates `IDENTITY` columns as `integer` ([SQLiteIdentityColumnSupport.java#L30-L32](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/identity/SQLiteIdentityColumnSupport.java#L30-L32)), then `validate` expects `bigint` for a `Long` id ([ColumnDefinitions.java#L28-L30](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/tool/schema/internal/ColumnDefinitions.java#L28-L30)).
- `@Lob byte[]`: Hibernate reads it with `ResultSet.getBlob` ([BlobJdbcType.java#L58-L62](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BlobJdbcType.java#L58-L62)), which sqlite-jdbc does not implement ([JDBC4ResultSet.java#L451-L453](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc4/JDBC4ResultSet.java#L451-L453)).
- Unique violation: the dialect's exception conversion does not map `SQLITE_CONSTRAINT` ([SQLiteDialect.java#L451-L470](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L451-L470)), so Spring raises `JpaSystemException` instead of `DataIntegrityViolationException`.

One limit was also found: `Instant` values kept milliseconds only, so the suite tests at millisecond precision.

### F1: generated identifiers

Every insert of an entity with an `IDENTITY` id failed with `SQLFeatureNotSupportedException` from `AvaticaConnection.prepareStatement`. The driver reports `supportsGetGeneratedKeys() == false`, and Hibernate accordingly uses its selecting delegate, which prepares the insert with `prepareStatement(sql, Statement.NO_GENERATED_KEYS)` ([AbstractSelectingDelegate.java#L60-L64](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/id/insert/AbstractSelectingDelegate.java#L60-L64)). Avatica, which the Arrow driver is built on, throws for that overload whatever the flag value ([AvaticaConnection.java#L373-L376](https://github.com/apache/calcite-avatica/blob/rel/avatica-1.26.0/core/src/main/java/org/apache/calcite/avatica/AvaticaConnection.java#L373-L376)). Hibernate and the driver disagree on a standard JDBC call, so this is a dialect-and-driver failure, and the server cannot change it.

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

The rule produces candidate B: skylite will ship its own JDBC driver over its HTTPS API. Candidate A failed 16 of 16 tests, including failures of the driver kind (transactions, F3) and of the dialect-and-driver kind (generated identifiers, F1), which count against it under the rule.

## Alternatives considered

- **Candidate A, Arrow Flight SQL with the Arrow JDBC driver.** Rejected by the rule above. Transactions failing in the driver was the deciding result; a skylite Flight SQL server could not fix F1 or F3.
- **Candidate A with a patched or forked Arrow driver.** Not tested. It would mean maintaining Java driver code anyway, which is what candidate A was meant to avoid, and the rule tested the driver as published.

## Consequences

- skylite has to write and maintain a JDBC driver that talks to its HTTPS API, and a Hibernate dialect. The suite in `clients/java/acceptance/` is the yardstick for both, and will run against skylite's endpoint once one exists.
- The three baseline failures are gaps in the community SQLite dialect and sqlite-jdbc. A skylite dialect that extends the community one would inherit them unless it addresses them; schema validation, `@Lob` and constraint violations are therefore requirements for skylite's dialect.
- The `java-acceptance` CI job runs the baseline on every pull request, merge-queue group and push to `main`. It is not a required check until the ruleset on `main` is changed to include it.
- `internal/spike/flightsql/` is a nested module kept to reproduce candidate A's result. `go build ./...` at the repository root does not include it.
- If this record is accepted, the [open question](/docs/architecture/#wire-protocol-for-jdbc) on the architecture page is to be updated in the same pull request.

## Limits

- One driver release (Arrow Flight SQL JDBC 19.0.0), one server (the Arrow Go v18.8.0 SQLite example), one JDK (Temurin 25) and one machine (Linux, local) were tested. In arrow-java's `main` branch at commit `a62942c0`, `commit` ([ArrowFlightMetaImpl.java#L57-L60](https://github.com/apache/arrow-java/blob/a62942c0c8ab73e7a86236c8d19321c7aebe07ff/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L57-L60)) and `rollback` ([ArrowFlightMetaImpl.java#L158-L161](https://github.com/apache/arrow-java/blob/a62942c0c8ab73e7a86236c8d19321c7aebe07ff/flight/flight-sql-jdbc-core/src/main/java/org/apache/arrow/driver/jdbc/ArrowFlightMetaImpl.java#L158-L161)) were still stubs on 2026-11-15. v19.0.0 was the latest release then.
- The spike server opens a SQLite file in WAL mode instead of the example's in-memory database with sample tables; everything else is the example server unchanged.
- Each test stopped at its first failure, so most later steps on candidate A were not reached. Results beyond the first failure come from the source and the separate probes, not from the suite.
- No server that returns parameter and result schemas was built, so how candidate A would fare against a more complete Flight SQL server is untested; F1 and F3 do not depend on the server.
- No HTTP/2 proxy, TLS, authentication or performance was tested.
- The baseline covers the suite's 16 tests only. It does not show that sqlite-jdbc or the community dialect work for features the suite does not test.

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
