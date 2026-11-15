package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;

/**
 * Each mapped column type round-trips at both ends of its range, before and after 1970, with empty values, and
 * with null in each column; a parameter rebound from a value to null binds null.
 */
class ColumnTypesTest extends AcceptanceTestBase {
    /** An apostrophe, accented letters, a character outside ASCII's range and one outside the 16-bit range. */
    private static final String TEXT = "O'Reilly: héllo, wörld ✓ 😀";
    /** More than 65,535 characters, and more than 64 KiB in both UTF-8 and UTF-16. */
    private static final String LONG_TEXT = "héllo, wörld ✓ 😀 ".repeat(5000);

    @Autowired DataSource dataSource;

    @Test
    void valuesRoundTrip() {
        byte[] payload = new byte[300 * 1024];
        new Random(42).nextBytes(payload);
        // sqlite-jdbc stores timestamps with millisecond precision; see README.md, "Known baseline limits".
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        // The top of each range.
        roundTrips(sample(TEXT, Long.MAX_VALUE, Math.PI, true, "1234567890.12", now,
                LocalDate.of(2026, 2, 28), payload));
        // The bottom of each range, a long string, and a date and time before 1970.
        roundTrips(sample(LONG_TEXT, Long.MIN_VALUE, -Double.MAX_VALUE, false, "-9876543210.98",
                Instant.parse("1969-07-20T20:17:40.123Z"), LocalDate.of(1969, 7, 20), new byte[] {1, 2, 3}));
        // Small negative numbers, the epoch itself, the empty string and an empty payload.
        roundTrips(sample("", -42L, -Math.E, false, "-0.01", Instant.EPOCH, LocalDate.EPOCH, new byte[0]));

        // An int column, in Event: with the community dialect, an Integer field in Sample makes Hibernate create
        // Sample's IDENTITY id without a type, so SQLite no longer fills it in (README.md, "Known baseline limits").
        List<Event> written = events.saveAll(List.of(new Event(Integer.MIN_VALUE), new Event(-7), new Event(Integer.MAX_VALUE)));
        assertThat(events.findAll()).extracting(Event::getId, Event::getSeq).containsExactlyInAnyOrderElementsOf(
                written.stream().map(e -> tuple(e.getId(), e.getSeq())).toList());
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

    /** One prepared statement executed twice: with values, then with each parameter set to null. */
    @Test
    void parameterReboundToNullBindsNull() throws Exception {
        try (Connection c = dataSource.getConnection();
                PreparedStatement insert = c.prepareStatement("insert into sample (text, number) values (?, ?)")) {
            insert.setString(1, "first");
            insert.setLong(2, 42);
            insert.executeUpdate();
            insert.setNull(1, Types.VARCHAR);
            insert.setNull(2, Types.BIGINT);
            insert.executeUpdate();
        }
        assertThat(samples.findAll(Sort.by("id"))).extracting(Sample::getText, Sample::getNumber)
                .containsExactly(tuple("first", 42L), tuple(null, null));
    }

    private void roundTrips(Sample written) {
        // A copy taken before saving: the driver receives the entity's own array and could change it.
        byte[] payload = written.getPayload().clone();
        Sample r = samples.findById(samples.save(written).getId()).orElseThrow();
        assertThat(r.getText()).isEqualTo(written.getText());
        assertThat(r.getNumber()).isEqualTo(written.getNumber());
        assertThat(r.getRatio()).isEqualTo(written.getRatio());
        assertThat(r.getFlag()).isEqualTo(written.getFlag());
        assertThat(r.getAmount()).isEqualByComparingTo(written.getAmount());
        assertThat(r.getCreatedAt()).isEqualTo(written.getCreatedAt());
        assertThat(r.getDay()).isEqualTo(written.getDay());
        assertThat(r.getPayload()).isEqualTo(payload);
    }

    private static Sample sample(String text, long number, double ratio, boolean flag, String amount,
            Instant createdAt, LocalDate day, byte[] payload) {
        Sample s = new Sample();
        s.setText(text);
        s.setNumber(number);
        s.setRatio(ratio);
        s.setFlag(flag);
        s.setAmount(new BigDecimal(amount));
        s.setCreatedAt(createdAt);
        s.setDay(day);
        s.setPayload(payload);
        return s;
    }
}
