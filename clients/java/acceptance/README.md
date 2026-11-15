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

Against another driver, set the three properties. Any of them can be overridden
on the command line:

```sh
mvn -f clients/java/acceptance/pom.xml test \
  -Djdbc.url=jdbc:... -Djdbc.driver=com.example.Driver \
  -Djdbc.dialect=org.hibernate.community.dialect.SQLiteDialect
```

The `flightsql` profile runs the suite through the Apache Arrow Flight SQL JDBC
driver against the server in `internal/spike/flightsql`:

```sh
(cd internal/spike/flightsql && go run . -db /tmp/flightsql.db) &
mvn -f clients/java/acceptance/pom.xml test -Pflightsql \
  -Djunit.jupiter.conditions.deactivate='org.junit.*DisabledCondition'
```

The last flag also runs the tests that are disabled because they fail on the
baseline (below).

## Configuration

`src/main/resources/application.properties` holds the settings every run
shares: `ddl-auto=create`, JDBC batch size 50 with ordered inserts, and a
HikariCP pool of up to 4 connections. Spring Boot's own properties
(`spring.datasource.url`, `spring.datasource.driver-class-name`,
`spring.jpa.database-platform`) can also be set through the environment, for
example `SPRING_DATASOURCE_URL`.

## What each test stands for

| Test | What it checks | Why an ORM needs it |
|---|---|---|
| `SchemaLifecycleTest.createThenValidate` | Start with `ddl-auto=create`, write a row, stop; start again with `ddl-auto=validate` and read the row back | Hibernate generates DDL from the entities, and `validate` compares the entities with the database's JDBC metadata (`getTables`, `getColumns`); applications commonly validate on start |
| `CrudTest.createReadUpdateDelete` | Insert two rows with `IDENTITY` ids, read, update and delete one | Database-generated ids reach Hibernate through `getGeneratedKeys`, `RETURNING` or an identity select, depending on dialect and driver |
| `RelationsTest.lazyAssociationsLoadInsideTransaction` | `@OneToMany` and lazy `@ManyToOne` are uninitialised after load and load on first access inside a transaction | Lazy loading issues further queries on the same connection and persistence context |
| `TransactionTest.commitIsVisibleFromSecondConnection` | A `@Transactional` service method saves an author and then each book in separate repository calls; a new connection outside the pool sees all rows | One transaction spans several statements, and a commit must make them all durable together |
| `TransactionTest.exceptionRollsBackEveryWrite` | The same writes, flushed, then an exception; a new connection outside the pool sees no rows | Spring rolls back on a runtime exception; a driver whose `rollback()` does nothing would leave partial writes behind |
| `QueryTest.derivedQuery` | `findByEmail`, `findByNameStartingWithOrderByNameAsc` | Spring Data turns method names into JPQL with `LIKE` and `ORDER BY` |
| `QueryTest.jpqlQuery` | `@Query` with `size()` on a collection | JPQL is translated to SQL with a correlated subquery |
| `QueryTest.nativeQuery` | `@Query(nativeQuery = true)` with a named parameter | Native SQL goes to the driver as written |
| `QueryTest.paginationAndSorting` | `findAll(PageRequest.of(1, 3, Sort...))` | Paging issues `LIMIT`/`OFFSET` from the dialect plus a `count` query |
| `OptimisticLockingTest.versionIncrementsAndStaleUpdateFails` | `@Version` increments, and saving a stale copy raises `ObjectOptimisticLockingFailureException` | Hibernate checks the update count of `update ... where version = ?`, so the driver must report it exactly |
| `BatchInsertTest.insertsAreBatched` | 500 inserts with JDBC batching on; Hibernate prepares the insert at most once per batch of 50 | Batching uses `addBatch`/`executeBatch` on a prepared statement; ids are application-generated UUIDs because Hibernate does not batch `IDENTITY` inserts |
| `ColumnTypesTest.valuesRoundTrip` | String (non-ASCII), long, double, boolean, `BigDecimal`, `Instant`, `LocalDate` and a 300 KiB `byte[]` in a `blob` column round-trip | Each Java type goes through a dialect type mapping and a specific `ResultSet` getter |
| `ColumnTypesTest.nullsRoundTrip` | The same columns, all null | Binding null needs the right SQL type from the dialect |
| `LobTest.lobRoundTrips` | A 300 KiB `@Lob byte[]` round-trips | `@Lob` makes Hibernate use `ResultSet.getBlob` |
| `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation` | A duplicate unique value raises Spring's `DataIntegrityViolationException` | Applications catch this exception; the dialect has to recognise the driver's error code |
| `ConnectionPoolTest.uncommittedWriteIsInvisibleToAnotherPooledConnection` | Two HikariCP connections held at once; an uncommitted insert on one is not visible on the other, and rollback removes it | A pool shares one database between threads, which relies on transaction isolation |

## Known baseline failures

Three tests fail on the baseline (Spring Boot 4.1.2, Hibernate ORM 7.4.13,
sqlite-jdbc 3.53.4.0) because of the Hibernate community SQLite dialect or
sqlite-jdbc, not because of anything skylite controls. They are kept, marked
`@Disabled` with the reason, and can be run with the flag shown above.

- `SchemaLifecycleTest.createThenValidate`: the dialect creates `IDENTITY`
  columns as `integer`
  ([SQLiteIdentityColumnSupport.java#L30-L32](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/identity/SQLiteIdentityColumnSupport.java#L30-L32)),
  and validation then expects `bigint` for a `Long` id and rejects the column
  ([ColumnDefinitions.java#L28-L30](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/tool/schema/internal/ColumnDefinitions.java#L28-L30)).
- `LobTest.lobRoundTrips`: Hibernate reads every BLOB JDBC type through
  `ResultSet.getBlob`
  ([BlobJdbcType.java#L58-L62](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-core/src/main/java/org/hibernate/type/descriptor/jdbc/BlobJdbcType.java#L58-L62)),
  including the binding the dialect registers
  ([SQLiteDialect.java#L367](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L367)),
  and sqlite-jdbc does not implement `getBlob`
  ([JDBC4ResultSet.java#L451-L453](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/jdbc4/JDBC4ResultSet.java#L451-L453)).
  A `byte[]` without `@Lob` in a `blob` column works and is covered by
  `ColumnTypesTest`.
- `ConstraintTest.duplicateUniqueValueIsDataIntegrityViolation`: sqlite-jdbc
  reports error code 19 (`SQLITE_CONSTRAINT`) with no SQL state
  ([SQLiteException.java#L33](https://github.com/xerial/sqlite-jdbc/blob/3.53.4.0/src/main/java/org/sqlite/SQLiteException.java#L33)),
  and the dialect's exception conversion does not map that code
  ([SQLiteDialect.java#L451-L470](https://github.com/hibernate/hibernate-orm/blob/7.4.13/hibernate-community-dialects/src/main/java/org/hibernate/community/dialect/SQLiteDialect.java#L451-L470)),
  so Hibernate throws `GenericJDBCException` and Spring turns it into
  `JpaSystemException`.

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
