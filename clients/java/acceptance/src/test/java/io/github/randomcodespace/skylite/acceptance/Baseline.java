package io.github.randomcodespace.skylite.acceptance;

import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * The reference stack: sqlite-jdbc with the Hibernate community SQLite dialect. Tests known to fail on it
 * are skipped there with {@code @DisabledIf("...Baseline#excludesKnownFailures")}, and run on every other
 * driver or dialect. {@code -Dacceptance.include-baseline-failures=true} runs them on the baseline too.
 * Registered as an extension, it prints each skipped test and its reason, so a run's log shows them.
 */
class Baseline implements TestWatcher {
    static final String CONDITION = "io.github.randomcodespace.skylite.acceptance.Baseline#excludesKnownFailures";

    /** Decided from the connection {@link ConnectionReport} verified at start-up, not from configuration. */
    static boolean excludesKnownFailures(ExtensionContext context) {
        if (Boolean.getBoolean("acceptance.include-baseline-failures")) {
            return false;
        }
        return SpringExtension.getApplicationContext(context).getBean(ConnectionReport.class).isBaseline();
    }

    static final String NO_SAVEPOINTS = "io.github.randomcodespace.skylite.acceptance.Baseline#lacksSavepoints";

    /**
     * Whether the verified connection's driver reports no savepoint support, as skylite's does: its server refuses
     * SAVEPOINT on a write channel. Never true on the baseline, whose driver supports them.
     */
    static boolean lacksSavepoints(ExtensionContext context) {
        return !SpringExtension.getApplicationContext(context).getBean(ConnectionReport.class).supportsSavepoints();
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
        System.out.printf("SKIPPED %s.%s: %s%n", context.getRequiredTestClass().getSimpleName(),
                context.getRequiredTestMethod().getName(), reason.orElse("no reason given"));
    }
}
