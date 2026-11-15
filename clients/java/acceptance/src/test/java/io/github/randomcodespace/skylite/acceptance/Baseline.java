package io.github.randomcodespace.skylite.acceptance;

import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

/**
 * The reference stack: sqlite-jdbc with the Hibernate community SQLite dialect. Tests known to fail on it
 * are skipped there with {@code @DisabledIf("...Baseline#excludesKnownFailures")}, and run on every other
 * driver or dialect. {@code -Dacceptance.include-baseline-failures=true} runs them on the baseline too.
 * Registered as an extension, it prints each skipped test and its reason, so a run's log shows them.
 */
class Baseline implements TestWatcher {
    static final String CONDITION = "io.github.randomcodespace.skylite.acceptance.Baseline#excludesKnownFailures";

    static boolean excludesKnownFailures() {
        // Surefire sets these system properties from -Djdbc.driver and -Djdbc.dialect (pom.xml).
        return "org.sqlite.JDBC".equals(System.getProperty("spring.datasource.driver-class-name"))
                && "org.hibernate.community.dialect.SQLiteDialect".equals(System.getProperty("spring.jpa.database-platform"))
                && !Boolean.getBoolean("acceptance.include-baseline-failures");
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        System.out.printf("SKIPPED %s.%s: %s%n", context.getRequiredTestClass().getSimpleName(),
                context.getRequiredTestMethod().getName(), reason.orElse("no reason given"));
    }
}
