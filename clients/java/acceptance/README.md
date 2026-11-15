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
`spring.datasource.driver-class-name` and `spring.jpa.database-platform`, and
system properties outrank environment variables in Spring Boot, so a setting
such as `SPRING_DATASOURCE_URL` would be ignored. The suite refuses to start if
one of those environment variables is set.

```sh
mvn -f clients/java/acceptance/pom.xml test \
  -Djdbc.url=jdbc:... -Djdbc.driver=com.example.Driver \
  -Djdbc.dialect=org.hibernate.community.dialect.SQLiteDialect
```

Each time the application starts, the suite logs the database it is connected
to, so a run's log shows which database it certified:

```text
ConnectionReport : Connected to jdbc:sqlite:.../target/baseline.db?journal_mode=WAL&busy_timeout=5000 with driver org.sqlite.JDBC (SQLite JDBC 3.53.4.0), dialect org.hibernate.community.dialect.SQLiteDialect
```

The URL in that line is the one the driver reports through
`DatabaseMetaData.getURL()`; the driver class and dialect are the configured
ones.

The `flightsql` profile runs the suite through the Apache Arrow Flight SQL JDBC
driver against the server in `internal/spike/flightsql`:

```sh
(cd internal/spike/flightsql && go run . -db /tmp/flightsql.db) &
mvn -f clients/java/acceptance/pom.xml test -Pflightsql
```

### Tests skipped on the baseline

Five tests are known to fail on the baseline stack, sqlite-jdbc with the
Hibernate community SQLite dialect (see "Known baseline failures" below). They
are skipped only when the driver is `org.sqlite.JDBC` and the dialect is
`org.hibernate.community.dialect.SQLiteDialect`; with any other driver or
dialect every test runs, with no flag. Each skipped test is printed with its
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

## What each test stands for

| Test | What it checks | Why an ORM needs it |
|---|---|---|
| `SchemaLifecycleTest.createThenValidate` | Start with `ddl-auto=create`, write a row, stop; start again with `ddl-auto=validate` and read the row back | Hibernate generates DDL from the entities, and `validate` compares the entities with the database's JDBC metadata (`getTables`, `getColumns`); applications commonly validate on start |
| `CrudTest.createReadUpdateDelete` | Insert two rows with `IDENTITY` ids, read, update and delete one | Database-generated ids reach Hibernate through `getGeneratedKeys`, `RETURNING` or an identity select, depending on dialect and driver |
| `RelationsTest.lazyAssociationsLoadInsideTransaction` | `@OneToMany` and lazy `@ManyToOne` are uninitialised after load and load on first access inside a transaction | Lazy loading issues further queries on the same connection and persistence context |
| `RelationsTest.joinFetchLoadsParentAndChildren` | A JPQL `join fetch` loads an author and its books in a fresh persistence context; the collection is initialised, and each book has its id and points to the same author instance | One result row holds columns of two tables, with several identifier columns, which Hibernate reads by alias and assembles into a parent and its children |
| `TransactionTest.commitIsVisibleFromSecondConnection` | A `@Transactional` service method saves an author and then each book in separate repository calls; a new connection outside the pool sees all rows | One transaction spans several statements, and a commit must make them all durable together |
| `TransactionTest.exceptionRollsBackEveryWrite` | The same writes, flushed, then an exception; a new connection outside the pool sees no rows | Spring rolls back on a runtime exception; a driver whose `rollback()` does nothing would leave partial writes behind |
| `QueryTest.derivedQuery` | `findByEmail`, `findByNameStartingWithOrderByNameAsc` | Spring Data turns method names into JPQL with `LIKE` and `ORDER BY` |
| `QueryTest.jpqlQuery` | `@Query` with `size()` on a collection | JPQL is translated to SQL with a correlated subquery |
| `QueryTest.nativeQuery` | `@Query(nativeQuery = true)` with a named parameter | Native SQL goes to the driver as written |
| `QueryTest.paginationAndSorting` | `findAll(PageRequest.of(1, 3, Sort...))` | Paging issues `LIMIT`/`OFFSET` from the dialect plus a `count` query |
| `OptimisticLockingTest.versionIncrementsAndStaleUpdateFails` | `@Version` increments, and saving a stale detached copy raises `ObjectOptimisticLockingFailureException` | Saving a detached copy goes through Hibernate's merge, which loads the current row and rejects the version before any `UPDATE` is sent; this test does not reach the driver's update count |
| `OptimisticLockingTest.updateMatchingNoRowFailsOnFlush` | Inside a transaction, a managed account is loaded, its stored version is advanced by plain JDBC on the same connection, and the entity is changed and flushed; the flush raises `ObjectOptimisticLockingFailureException` and the row keeps its balance | The flush sends `update ... where id = ? and version = ?`, which matches no row; only the update count of 0 from the driver tells Hibernate the update failed. A driver that reports 1 for every update fails this test |
| `BatchInsertTest.insertsAreBatched` | 500 inserts with JDBC batching on; Hibernate prepares the insert at most once per batch of 50 | Batching uses `addBatch`/`executeBatch` on a prepared statement; ids are application-generated UUIDs because Hibernate does not batch `IDENTITY` inserts |
| `ColumnTypesTest.valuesRoundTrip` | String (non-ASCII), long, double, boolean, `BigDecimal`, `Instant`, `LocalDate` and a 300 KiB `byte[]` in a `blob` column round-trip | Each Java type goes through a dialect type mapping and a specific `ResultSet` getter |
| `ColumnTypesTest.nullsRoundTrip` | The same columns, all null | Binding null needs the right SQL type from the dialect |
| `LobTest.lobRoundTrips` | A 300 KiB `@Lob byte[]` round-trips | With the community dialect, Hibernate reads this mapping through `ResultSet.getBlob`; see "Known baseline failures" |
| `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation` | A duplicate unique value raises Spring's `DataIntegrityViolationException` | Applications catch this exception; Hibernate has to recognise the driver's error as a constraint violation |
| `ConstraintTest.referenceToMissingRowIsDataIntegrityViolation` | Saving a book whose author id does not exist raises `DataIntegrityViolationException`, and no book is stored | The schema Hibernate generates must declare the foreign key, and the database must enforce it |
| `ConstraintTest.nullInRequiredColumnIsDataIntegrityViolation` | A JPQL update that sets a `nullable = false` column to null raises `DataIntegrityViolationException`, and the row is unchanged | Hibernate checks nulls itself before an insert, but not in a JPQL update, so here the database's `not null` constraint and the error conversion are tested |
| `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` | Two HikariCP connections held at once; an uncommitted insert on one is not visible on the other, and rollback removes it | A pool shares one database between threads, which relies on transaction isolation |

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
