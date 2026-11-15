package io.github.randomcodespace.skylite.acceptance.selfcheck;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Checks the acceptance suite itself: runs the whole suite once through {@link FaultyDriver} with no fault, which
 * must pass every test with none skipped, and once per fault, which must fail at least one test. Each run is a
 * separate JVM; runs go in parallel. Writes the table of faults and the tests that caught them to
 * {@code target/fault-check.md}. Run with {@code make java-fault-check}; {@code -Dacceptance.faults=a,b} runs a
 * subset.
 */
class FaultCheck {
    private static final Pattern TESTS = Pattern.compile("^TESTS found=(\\d+) failed=(\\d+) skipped=(\\d+)$", Pattern.MULTILINE);
    private static final Pattern INJECTED = Pattern.compile("^FAULT \\S+ injected=(\\d+)$", Pattern.MULTILINE);
    private static final Pattern FAILED = Pattern.compile("^FAILED (\\S+?):", Pattern.MULTILINE);

    /**
     * Faults that break DDL start from a copy of the control run's database, which holds the whole schema, so that
     * only Hibernate halting on the failed statements can make them fail; on an empty file the missing tables would.
     */
    private static final Set<String> FROM_CONTROL_DATABASE = Set.of("ddl-unsupported");

    record Run(String fault, int found, int failed, int skipped, int injected, List<String> caughtBy, Path log) {}

    @Test
    void everyFaultIsCaught() throws Exception {
        Path dir = Path.of("target", "fault-check").toAbsolutePath();
        Files.createDirectories(dir);
        String only = System.getProperty("acceptance.faults", "");
        List<String> faults = new ArrayList<>(List.of("none"));
        faults.addAll(only.isBlank() ? FaultyDriver.FAULTS : List.of(only.split(",")));

        int parallel = Math.max(2, Runtime.getRuntime().availableProcessors());
        ExecutorService pool = Executors.newFixedThreadPool(parallel);
        Map<String, Future<Run>> futures = new LinkedHashMap<>();
        for (String fault : faults) {
            Future<Run> control = futures.get("none");
            futures.put(fault, pool.submit(() -> run(fault, dir, FROM_CONTROL_DATABASE.contains(fault) ? control : null)));
        }
        pool.shutdown();
        List<Run> runs = new ArrayList<>();
        for (Future<Run> f : futures.values()) {
            runs.add(f.get());
        }

        StringBuilder table = new StringBuilder("| Fault | Fault applied | Tests failed | Caught by |\n|---|---|---|---|\n");
        for (Run r : runs) {
            table.append("| `").append(r.fault()).append("` | ").append(r.injected()).append(" | ")
                    .append(r.failed()).append(" of ").append(r.found()).append(" | ")
                    .append(r.caughtBy().isEmpty() ? "**nothing**" : String.join(", ", r.caughtBy().stream().map(t -> "`" + t + "`").toList()))
                    .append(" |\n");
        }
        Files.writeString(dir.resolve("..").resolve("fault-check.md"), table);
        System.out.println(table);

        Run control = runs.getFirst();
        assertThat(control.found()).as("tests found in the control run, see %s", control.log()).isPositive();
        assertThat(control.failed()).as("failures with no fault, see %s", control.log()).isZero();
        assertThat(control.skipped()).as("skipped tests with no fault, see %s", control.log()).isZero();
        for (Run r : runs.subList(1, runs.size())) {
            assertThat(r.found()).as("tests found with fault %s, see %s", r.fault(), r.log()).isEqualTo(control.found());
            assertThat(r.injected()).as("times fault %s was applied, see %s", r.fault(), r.log()).isPositive();
            assertThat(r.failed()).as("failures with fault %s, see %s", r.fault(), r.log()).isPositive();
        }
    }

    private static Run run(String fault, Path dir, Future<Run> control) throws Exception {
        Path log = dir.resolve(fault + ".log");
        Path db = dir.resolve(fault + ".db");
        for (String suffix : List.of("", "-wal", "-shm")) {
            Files.deleteIfExists(Path.of(db + suffix));
        }
        if (control != null) {
            control.get();
            Files.copy(dir.resolve("none.db"), db);
        }
        Process p = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                "-Djdbc.url=" + FaultyDriver.PREFIX + "jdbc:sqlite:" + db + "?journal_mode=WAL&busy_timeout=5000",
                "-Djdbc.driver=" + FaultyDriver.class.getName(),
                "-Djdbc.dialect=org.hibernate.community.dialect.SQLiteDialect",
                "-Dacceptance.fault=" + fault,
                SuiteMain.class.getName())
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        if (!p.waitFor(10, TimeUnit.MINUTES)) {
            p.destroyForcibly();
        }
        String out = Files.readString(log);
        Matcher tests = TESTS.matcher(out);
        Matcher injected = INJECTED.matcher(out);
        boolean done = tests.find();
        List<String> caughtBy = FAILED.matcher(out).results().map(m -> m.group(1)).distinct().sorted().toList();
        return new Run(fault, done ? Integer.parseInt(tests.group(1)) : -1, done ? Integer.parseInt(tests.group(2)) : -1,
                done ? Integer.parseInt(tests.group(3)) : -1, injected.find() ? Integer.parseInt(injected.group(1)) : 0,
                caughtBy, log);
    }
}
