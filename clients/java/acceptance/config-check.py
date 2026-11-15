#!/usr/bin/env python3
"""Configuration check for the acceptance suite: `make java-config-check`.

Runs CrudTest and TransactionTest once per case below, each time pointing the
suite at the requested database with -Djdbc.url and, through another source,
at a second SQLite file that holds a table `author` with one sentinel row,
which Hibernate's ddl-auto=create would drop. Each case must end as expected:

  rejected   start-up fails with the configuration check's message
  hibernate  start-up fails inside Hibernate, before any connection is opened
  requested  the tests pass, and the log names the requested database
  control    the second file is itself the requested database (it loses its row)

and, except for the control, the sentinel row must still be there. The tests
run in a copy of the project under target/config-check, because some cases
add configuration files to it. Needs Python 3, a JDK and Maven. Prints one
line per case and exits non-zero if any case differs from what is expected.
Case names given as arguments run only those cases.
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

here = Path(__file__).resolve().parent
work = here / 'target' / 'config-check'
project = work / 'project'
requested = f'jdbc:sqlite:{work}/requested.db?journal_mode=WAL&busy_timeout=5000'
other_db = work / 'other.db'
other = f'jdbc:sqlite:{other_db}'
wrapper = 'io.github.randomcodespace.skylite.acceptance.selfcheck.FaultyDriver'
wrapped_other = 'jdbc:faulty:' + other
simple = 'org.springframework.jdbc.datasource.SimpleDriverDataSource'
h2 = 'org.hibernate.dialect.H2Dialect'
provider = 'org.hibernate.engine.jdbc.connections.internal.DriverManagerConnectionProviderImpl'
guard = ('the application is not connected to the requested database', 'not HikariCP')


def json_env(settings):
    return {'SPRING_APPLICATION_JSON': json.dumps(settings)}


# (name, expected result, extra Maven arguments, extra environment, file to add to the project copy)
cases = [
    ('maven-bypass', 'rejected', ['-Djdbc.url=jdbc:sqlite:/nonexistent/requested.db', '-Dspring.datasource.url=' + wrapped_other,
                                  '-Dspring.datasource.driver-class-name=' + wrapper,
                                  '-Dspring.jpa.database-platform=org.hibernate.community.dialect.SQLiteDialect'], {}, None),
    ('maven-url', 'rejected', ['-Dspring.datasource.url=' + other], {}, None),
    ('maven-url-and-driver', 'rejected', ['-Dspring.datasource.url=' + wrapped_other,
                                          '-Dspring.datasource.driver-class-name=' + wrapper], {}, None),
    ('maven-driver', 'rejected', ['-Dspring.datasource.driver-class-name=' + wrapper], {}, None),
    ('maven-hikari-url', 'rejected', ['-Dspring.datasource.hikari.jdbc-url=' + other], {}, None),
    ('maven-hikari-driver', 'rejected', ['-Dspring.datasource.hikari.driver-class-name=' + wrapper], {}, None),
    ('maven-type', 'rejected', ['-Dspring.datasource.type=' + simple], {}, None),
    ('maven-dialect', 'rejected', ['-Dspring.jpa.database-platform=' + h2], {}, None),
    ('maven-properties-dialect', 'rejected', ['-Dspring.jpa.properties.hibernate.dialect=' + h2], {}, None),
    ('maven-hibernate-provider', 'hibernate', ['-Dhibernate.connection.provider_class=' + provider,
                                               '-Dhibernate.connection.url=' + other], {}, None),
    ('maven-hibernate-provider-jakarta-url', 'hibernate', ['-Dhibernate.connection.provider_class=' + provider,
                                                           '-Djakarta.persistence.jdbc.url=' + other], {}, None),
    ('maven-spring-jpa-provider', 'rejected', ['-Dspring.jpa.properties.hibernate.connection.provider_class=' + provider,
                                               '-Dspring.jpa.properties.hibernate.connection.url=' + other], {}, None),
    ('maven-spring-jpa-provider-jakarta-url', 'rejected', ['-Dspring.jpa.properties.hibernate.connection.provider_class=' + provider,
                                                           '-Dspring.jpa.properties.jakarta.persistence.jdbc.url=' + other], {}, None),
    # Found in the fourth review: the pool takes hikari.jdbc-url, which is the requested database, and Spring's
    # spring.datasource.url points elsewhere. Connections the tests open outside the pool must not follow it.
    ('pool-url-masks-spring-url', 'requested', ['-Dspring.datasource.url=' + other,
                                                '-Dspring.datasource.hikari.jdbc-url=' + requested], {}, None),
    ('env-url', 'rejected', [], {'SPRING_DATASOURCE_URL': other}, None),
    ('env-hikari-url', 'rejected', [], {'SPRING_DATASOURCE_HIKARI_JDBC_URL': other}, None),
    ('env-driver', 'rejected', [], {'SPRING_DATASOURCE_DRIVER_CLASS_NAME': wrapper}, None),
    ('env-type', 'rejected', [], {'SPRING_DATASOURCE_TYPE': simple}, None),
    ('env-dialect', 'rejected', [], {'SPRING_JPA_DATABASE_PLATFORM': h2}, None),
    ('env-jdbc-url', 'requested', [], {'JDBC_URL': other}, None),
    ('json-url', 'rejected', [], json_env({'spring.datasource.url': other}), None),
    ('json-url-and-driver', 'rejected', [], json_env({'spring.datasource.url': wrapped_other,
                                                      'spring.datasource.driver-class-name': wrapper}), None),
    ('json-hikari-url', 'rejected', [], json_env({'spring.datasource.hikari.jdbc-url': other}), None),
    ('json-jdbc-url', 'rejected', [], json_env({'jdbc.url': other}), None),
    ('json-driver', 'rejected', [], json_env({'spring.datasource.driver-class-name': wrapper}), None),
    ('json-type', 'rejected', [], json_env({'spring.datasource.type': simple}), None),
    ('json-dialect', 'rejected', [], json_env({'spring.jpa.database-platform': h2}), None),
    ('json-properties-dialect', 'rejected', [], json_env({'spring.jpa.properties.hibernate.dialect': h2}), None),
    ('json-provider', 'rejected', [], json_env({'spring.jpa.properties.hibernate.connection.provider_class': provider,
                                                'spring.jpa.properties.hibernate.connection.url': other}), None),
    ('java-tool-options-url', 'rejected', [], {'JAVA_TOOL_OPTIONS': '-Dspring.datasource.url=' + other}, None),
    ('java-tool-options-jdbc-url', 'requested', [], {'JAVA_TOOL_OPTIONS': '-Djdbc.url=' + other}, None),
    ('argline-url', 'rejected', ['-Djdbc.argLine=-Dspring.datasource.url=' + other], {}, None),
    ('argline-jdbc-url', 'requested', ['-Djdbc.argLine=-Djdbc.url=' + other], {}, None),
    ('additional-location-url', 'rejected', ['-Dspring.config.additional-location=file:' + str(work / 'url.properties')], {}, None),
    ('additional-location-hikari-url', 'rejected', ['-Dspring.config.additional-location=file:' + str(work / 'hikari.properties')], {}, None),
    ('profile-config', 'rejected', ['-Dspring.profiles.active=review'], {}, 'src/test/resources/application-review.properties'),
    ('working-directory-config', 'rejected', [], {}, 'application.properties'),
    ('working-directory-config-dir', 'rejected', [], {}, 'config/application.properties'),
    ('control-requested-other', 'control', ['-Djdbc.url=' + other], {}, None),
]


def seed():
    for suffix in ('', '-wal', '-shm'):
        Path(str(other_db) + suffix).unlink(missing_ok=True)
    with sqlite3.connect(other_db) as db:
        db.execute('create table author (id integer primary key, name text, email text)')
        db.execute("insert into author values (8765, 'sentinel', 'sentinel@example.com')")
    db.close()


def sentinel():
    with sqlite3.connect(other_db) as db:
        found = db.execute("select count(*) from sqlite_master where name = 'author'").fetchone()[0]
        count = db.execute('select count(*) from author where id = 8765').fetchone()[0] if found else 0
    db.close()
    return count == 1


def main():
    shutil.rmtree(work, ignore_errors=True)
    project.mkdir(parents=True)
    shutil.copy(here / 'pom.xml', project)
    shutil.copytree(here / 'src', project / 'src')
    (work / 'url.properties').write_text('spring.datasource.url=' + other + '\n')
    (work / 'hikari.properties').write_text('spring.datasource.hikari.jdbc-url=' + other + '\n')
    base = ['mvn', '-B', '-ntp', '-f', str(project / 'pom.xml'), 'test', '-Dtest=CrudTest,TransactionTest',
            '-Djdbc.url=' + requested, '-Djdbc.driver=org.sqlite.JDBC',
            '-Djdbc.dialect=org.hibernate.community.dialect.SQLiteDialect']
    # Compile once, so each case only runs the tests.
    subprocess.run(['mvn', '-B', '-ntp', '-q', '-f', str(project / 'pom.xml'), 'test-compile'], check=True)
    selected = [c for c in cases if not sys.argv[1:] or c[0] in sys.argv[1:]]
    wrong = []
    for name, expected, arguments, environment, added in selected:
        seed()
        path = project / added if added else None
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text('spring.datasource.url=' + other + '\n')
        log = work / (name + '.log')
        with log.open('w') as output:
            exit_code = subprocess.run(base + arguments, cwd=project, env=dict(os.environ, **environment),
                                       stdout=output, stderr=subprocess.STDOUT, timeout=300).returncode
        if path:
            path.unlink()
        text = log.read_text()
        kept = sentinel()
        rejected = any(g in text for g in guard)
        connected = 'ConnectionReport' in text and 'Connected to ' in text
        actual = ('control' if exit_code == 0 and not kept and 'Connected to ' + other in text else
                  'requested' if exit_code == 0 and 'Connected to ' + requested in text else
                  'rejected' if exit_code != 0 and rejected else
                  'hibernate' if exit_code != 0 and not connected else
                  'other')
        ok = actual == expected and (kept or expected == 'control')
        if not ok:
            wrong.append(name)
        print(f"{'ok  ' if ok else 'FAIL'} {name}: expected {expected}, got {actual}, exit {exit_code}, "
              f"sentinel {'kept' if kept else 'gone'}, log {log}", flush=True)
    print(f'{len(selected) - len(wrong)} of {len(selected)} cases as expected')
    return 1 if wrong else 0


if __name__ == '__main__':
    sys.exit(main())
