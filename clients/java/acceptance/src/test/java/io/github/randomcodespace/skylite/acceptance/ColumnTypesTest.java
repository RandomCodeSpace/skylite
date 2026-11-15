package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import org.junit.jupiter.api.Test;

/** Each mapped column type round-trips, and so do false, the empty string, and null in each column. */
class ColumnTypesTest extends AcceptanceTestBase {
    @Test
    void valuesRoundTrip() {
        byte[] payload = new byte[300 * 1024];
        new Random(42).nextBytes(payload);
        // sqlite-jdbc stores timestamps with millisecond precision; see README.md, "Known baseline limits".
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        Sample s = new Sample();
        s.setText("héllo, wörld ✓");
        s.setNumber(Long.MAX_VALUE - 1);
        s.setRatio(Math.PI);
        s.setFlag(true);
        s.setAmount(new BigDecimal("1234567890.12"));
        s.setCreatedAt(now);
        s.setDay(LocalDate.of(2026, 2, 28));
        s.setPayload(payload);
        Long id = samples.save(s).getId();

        // A second row with false, an empty string and a different payload.
        Sample f = new Sample();
        f.setText("");
        f.setFlag(false);
        f.setPayload(new byte[] {1, 2, 3});
        Long falseId = samples.save(f).getId();

        Sample r = samples.findById(id).orElseThrow();
        assertThat(r.getText()).isEqualTo("héllo, wörld ✓");
        assertThat(r.getNumber()).isEqualTo(Long.MAX_VALUE - 1);
        assertThat(r.getRatio()).isEqualTo(Math.PI);
        assertThat(r.getFlag()).isTrue();
        assertThat(r.getAmount()).isEqualByComparingTo("1234567890.12");
        assertThat(r.getCreatedAt()).isEqualTo(now);
        assertThat(r.getDay()).isEqualTo(LocalDate.of(2026, 2, 28));
        assertThat(r.getPayload()).isEqualTo(payload);

        Sample rf = samples.findById(falseId).orElseThrow();
        assertThat(rf.getText()).isNotNull().isEmpty();
        assertThat(rf.getFlag()).isNotNull().isFalse();
        assertThat(rf.getPayload()).isEqualTo(new byte[] {1, 2, 3});
    }

    @Test
    void nullsRoundTrip() {
        Long id = samples.save(new Sample()).getId();
        Sample r = samples.findById(id).orElseThrow();
        assertThat(r.getText()).isNull();
        assertThat(r.getNumber()).isNull();
        assertThat(r.getRatio()).isNull();
        assertThat(r.getFlag()).isNull();
        assertThat(r.getAmount()).isNull();
        assertThat(r.getCreatedAt()).isNull();
        assertThat(r.getDay()).isNull();
        assertThat(r.getPayload()).isNull();
    }
}
