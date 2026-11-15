package io.github.randomcodespace.skylite.acceptance.selfcheck;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectPackage;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.engine.discovery.PackageNameFilter;
import org.junit.platform.engine.support.descriptor.MethodSource;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

/** Runs the acceptance suite in this JVM and prints each failed test and a summary, for {@link FaultCheck}. */
public final class SuiteMain {
    static final String SUITE = "io.github.randomcodespace.skylite.acceptance";

    public static void main(String[] args) {
        SummaryGeneratingListener summary = new SummaryGeneratingListener();
        TestExecutionListener failures = new TestExecutionListener() {
            @Override
            public void executionFinished(TestIdentifier test, TestExecutionResult result) {
                if (result.getStatus() != TestExecutionResult.Status.SUCCESSFUL) {
                    String name = test.getSource().filter(MethodSource.class::isInstance).map(MethodSource.class::cast)
                            .map(m -> m.getClassName().substring(SUITE.length() + 1) + "." + m.getMethodName())
                            .orElse(test.getDisplayName());
                    System.out.println("FAILED " + name + ": " + result.getThrowable().map(Throwable::toString)
                            .orElse("").replace('\n', ' '));
                }
            }
        };
        LauncherFactory.create().execute(request()
                .selectors(selectPackage(SUITE))
                .filters(PackageNameFilter.excludePackageNames(SuiteMain.class.getPackageName()))
                .build(), summary, failures);
        TestExecutionSummary s = summary.getSummary();
        System.out.printf("TESTS found=%d failed=%d skipped=%d%n", s.getTestsFoundCount(),
                s.getTotalFailureCount(), s.getTestsSkippedCount() + s.getTestsAbortedCount());
        System.exit(0);
    }
}
