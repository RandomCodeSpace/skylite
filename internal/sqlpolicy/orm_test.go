package sqlpolicy

import (
	"testing"
)

type stmt struct {
	sql  string
	args []any
}

// hibernate is the DDL Hibernate ORM 7.4.13 with the community SQLite dialect
// generated for the JPA acceptance suite in clients/java/acceptance (ddl-auto
// create, captured with spring.jpa.show-sql), then the DML it issued.
var hibernate = []stmt{
	{sql: "drop table if exists account"},
	{sql: "drop table if exists author"},
	{sql: "drop table if exists book"},
	{sql: "drop table if exists document"},
	{sql: "drop table if exists event"},
	{sql: "drop table if exists sample"},
	{sql: "create table account (balance bigint not null, id integer, version bigint not null, primary key (id))"},
	{sql: "create table author (id integer, email varchar(255) not null unique, name varchar(255) not null, primary key (id))"},
	{sql: "create table book (author_id bigint not null, id integer, title varchar(255), primary key (id))"},
	{sql: "create table document (id integer, content blob, primary key (id))"},
	{sql: "create table event (seq integer not null, id blob not null, primary key (id))"},
	{sql: "create table sample (amount numeric(12,2), day date, flag boolean, ratio float, created_at timestamp, id integer, number bigint, text varchar(1048576), payload blob, primary key (id))"},
	{sql: "insert into author (email,name) values (?,?)", args: []any{"a@example.com", "A"}},
	{sql: "select last_insert_rowid()"},
	{sql: "insert into book (author_id,title) values (?,?)", args: []any{1, "T"}},
	{sql: "select a1_0.id,a1_0.email,a1_0.name,b1_0.author_id,b1_0.id,b1_0.title from author a1_0 left join book b1_0 on a1_0.id=b1_0.author_id where a1_0.id=?", args: []any{1}},
	{sql: "update author set email=?,name=? where id=?", args: []any{"b@example.com", "B", 1}},
	{sql: "delete from book where id=?", args: []any{1}},
}

// jdbcMetadata is what the reference driver, sqlite-jdbc 3.53.4.0, runs for
// the DatabaseMetaData calls Hibernate's schema validation makes: getTables,
// getColumns, getPrimaryKeys, getImportedKeys and getIndexInfo
// (src/main/java/org/sqlite/jdbc3/JDBC3DatabaseMetaData.java). skylite's own
// driver has to answer the same calls.
var jdbcMetadata = []stmt{
	{sql: `SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, NAME AS TABLE_NAME, TYPE AS TABLE_TYPE, NULL AS REMARKS, NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, NULL AS REF_GENERATION FROM (
    SELECT 'sqlite_schema' AS NAME, 'SYSTEM TABLE' AS TYPE UNION ALL
    SELECT NAME, UPPER(TYPE) AS TYPE FROM sqlite_schema WHERE NAME NOT LIKE 'sqlite\_%' ESCAPE '\' AND UPPER(TYPE) IN ('TABLE', 'VIEW') UNION ALL
    SELECT NAME, 'GLOBAL TEMPORARY' AS TYPE FROM sqlite_temp_master UNION ALL
    SELECT NAME, 'SYSTEM TABLE' AS TYPE FROM sqlite_schema WHERE NAME LIKE 'sqlite\_%' ESCAPE '\'
  ) WHERE TABLE_NAME LIKE '%' ESCAPE '\' AND TABLE_TYPE IN ('TABLE') ORDER BY TABLE_TYPE, TABLE_NAME;`},
	{sql: "SELECT LIKE('%autoincrement%', LOWER(sql)) FROM sqlite_schema WHERE LOWER(name) = LOWER('author') AND TYPE IN ('table', 'view')"},
	{sql: "PRAGMA table_xinfo('author')"},
	{sql: "pragma table_info('author');"},
	{sql: "select sql from sqlite_schema where lower(name) = lower('author') and type in ('table', 'view')"},
	{sql: "pragma foreign_key_list('book');"},
	{sql: "pragma index_list('author');"},
	{sql: "pragma index_info('sqlite_autoindex_author_1');"},
	{sql: "select name from sqlite_schema where type = 'table'"},
	{sql: "select sqlite_version()"},
}

// gorm is what GORM v1.31.2 with its SQLite dialector, gorm.io/driver/sqlite
// v1.6.0 (migrator.go), runs for AutoMigrate on a new and then an existing
// schema, and for Migrator().GetIndexes.
var gorm = []stmt{
	{sql: "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", args: []any{"companies"}},
	{sql: "CREATE TABLE `companies` (`id` integer PRIMARY KEY AUTOINCREMENT,`name` text)"},
	{sql: "SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", args: []any{"users"}},
	{sql: "CREATE TABLE `users` (`id` integer PRIMARY KEY AUTOINCREMENT,`created_at` datetime,`updated_at` datetime,`deleted_at` datetime,`name` text,`age` integer,`company_id` integer,CONSTRAINT `fk_users_company` FOREIGN KEY (`company_id`) REFERENCES `companies`(`id`))"},
	{sql: "CREATE INDEX `idx_users_deleted_at` ON `users`(`deleted_at`)"},
	{sql: "SELECT sql FROM sqlite_master WHERE type IN (?,?) AND tbl_name = ? AND sql IS NOT NULL order by type = ? desc", args: []any{"table", "index", "users", "table"}},
	{sql: "SELECT * FROM `users` LIMIT 1"},
	{sql: "SELECT count(*) FROM sqlite_master WHERE type = ? AND tbl_name = ? AND (sql LIKE ? OR sql LIKE ? OR sql LIKE ? OR sql LIKE ? OR sql LIKE ?)", args: []any{"table", "users", `%CONSTRAINT "fk_users_company" %`, "%CONSTRAINT fk_users_company %", "%CONSTRAINT `fk_users_company`%", "%CONSTRAINT [fk_users_company]%", "%CONSTRAINT \tfk_users_company\t%"}},
	{sql: "SELECT count(*) FROM sqlite_master WHERE type = ? AND tbl_name = ? AND name = ?", args: []any{"index", "users", "idx_users_deleted_at"}},
	{sql: "SELECT count(*) FROM sqlite_master WHERE type = ? AND tbl_name = ? AND (sql LIKE ? OR sql LIKE ? OR sql LIKE ? OR sql LIKE ? OR sql LIKE ?)", args: []any{"table", "users", `%"email" %`, "%email %", "%`email`%", "%[email]%", "%\temail\t%"}},
	{sql: "ALTER TABLE `users` ADD `email` text NOT NULL DEFAULT ''"},
	{sql: "SELECT * FROM PRAGMA_index_list(?)", args: []any{"users"}},
	{sql: "SELECT name FROM PRAGMA_index_info(?)", args: []any{"idx_users_deleted_at"}},
	{sql: "INSERT INTO `companies` (`name`) VALUES (?) RETURNING `id`", args: []any{"C"}},
	{sql: "INSERT INTO `users` (`created_at`,`updated_at`,`deleted_at`,`name`,`age`,`company_id`,`email`) VALUES (?,?,NULL,?,?,?,?) RETURNING `id`", args: []any{"2026-11-15 00:00:00", "2026-11-15 00:00:00", "U", 30, 1, "u@example.com"}},
	{sql: "SELECT * FROM `users` WHERE `users`.`deleted_at` IS NULL ORDER BY `users`.`id` LIMIT 1"},
	{sql: "DROP TABLE IF EXISTS `users`"},
}

func TestORMStatements(t *testing.T) {
	for name, stmts := range map[string][]stmt{"hibernate": hibernate, "jdbc metadata": jdbcMetadata, "gorm": gorm} {
		t.Run(name, func(t *testing.T) {
			both(t, Limits{}, func(t *testing.T, db *testDB) {
				// The metadata queries run against the acceptance suite's schema.
				for _, s := range append(hibernate[:12:12], stmts...) {
					if _, err := db.rows(s.sql, s.args...); err != nil {
						t.Fatalf("%.100s: %v", s.sql, err)
					}
				}
				db.checkFiles(t)
			})
		})
	}
}

func TestTempSchemaStaysInMemory(t *testing.T) {
	both(t, Limits{}, func(t *testing.T, db *testDB) {
		db.mustRun(t, "CREATE TABLE t(a)")
		before := db.owned(t)
		// Enough data to spill SQLite's page cache, so a temporary file would
		// be opened if temporary storage were on disk.
		db.mustRun(t, `
			CREATE TEMP TABLE tt(a);
			INSERT INTO tt SELECT randomblob(4000) FROM generate_series(1, 2000);
			CREATE INDEX temp.tti ON tt(a);
			CREATE TEMP VIEW tv AS SELECT count(*) FROM tt;
			CREATE TEMP TRIGGER tr AFTER INSERT ON main.t BEGIN INSERT INTO tt VALUES (new.a); END;
			INSERT INTO t VALUES (1);
			SELECT * FROM tv;
			SELECT length(a) FROM tt ORDER BY random() LIMIT 1;
			PRAGMA temp.table_info(tt);
			SELECT * FROM temp.sqlite_schema;
			DROP TRIGGER tr;
			DROP VIEW tv;
			DROP TABLE tt`)
		if after := db.owned(t); after["temp_store"] != before["temp_store"] || after["attached"] != "" {
			t.Errorf("temp_store %s, attached %q", after["temp_store"], after["attached"])
		}
		db.checkFiles(t)
	})
}

// GORM's AlterColumn on a table that others reference (gorm.io/driver/sqlite
// v1.6.0, migrator.go RunWithoutForeignKey and recreateTable): it turns
// foreign keys off, rebuilds the table in a transaction, which the driver
// opens and commits through the protocol, and turns them on again. The
// referencing rows survive. DropColumn, CreateConstraint and DropConstraint
// rebuild the same way without turning foreign keys off, and the cascade
// then deletes them.
func TestGORMTableRebuild(t *testing.T) {
	for _, fkOff := range []bool{true, false} {
		t.Run(map[bool]string{true: "AlterColumn", false: "DropColumn"}[fkOff], func(t *testing.T) {
			both(t, Limits{}, func(t *testing.T, db *testDB) {
				db.mustRun(t, "CREATE TABLE `companies` (`id` integer PRIMARY KEY AUTOINCREMENT,`name` text);"+
					"CREATE TABLE `users` (`id` integer PRIMARY KEY AUTOINCREMENT,`name` text,`company_id` integer,CONSTRAINT `fk_users_company` FOREIGN KEY (`company_id`) REFERENCES `companies`(`id`) ON DELETE CASCADE);"+
					"INSERT INTO companies(name) VALUES ('c1'), ('c2'); INSERT INTO users(name, company_id) VALUES ('a', 1), ('b', 1), ('c', 2)")
				if fkOff {
					if r, err := db.rows("PRAGMA foreign_keys"); err != nil || r[0][0] != "1" {
						t.Fatalf("PRAGMA foreign_keys: %v %v", r, err)
					}
					db.mustRun(t, "PRAGMA foreign_keys = OFF")
				}
				db.server(t, "BEGIN")
				db.mustRun(t, "CREATE TABLE `companies__temp` (`id` integer PRIMARY KEY AUTOINCREMENT,`name` varchar(100))")
				db.mustRun(t, "INSERT INTO `companies__temp`(`id`,`name`) SELECT `id`,`name` FROM `companies`")
				db.mustRun(t, "DROP TABLE `companies`")
				db.mustRun(t, "ALTER TABLE `companies__temp` RENAME TO `companies`")
				db.server(t, "COMMIT")
				if fkOff {
					db.mustRun(t, "PRAGMA foreign_keys = ON")
				}
				want := map[bool]string{true: "3", false: "0"}[fkOff]
				if r, err := db.rows("SELECT count(*) FROM users"); err != nil || r[0][0] != want {
					t.Fatalf("users: %v %v, want %s rows", r, err, want)
				}
			})
		})
	}
}
