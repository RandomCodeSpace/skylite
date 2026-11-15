package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

/** Hibernate creates the schema on a first start and validates it against database metadata on a second. */
@ExtendWith(Baseline.class)
class SchemaLifecycleTest {
    private static ConfigurableApplicationContext start(String ddlAuto) {
        return new SpringApplicationBuilder(AcceptanceApplication.class)
                .web(WebApplicationType.NONE)
                .run("--spring.jpa.hibernate.ddl-auto=" + ddlAuto); // a command-line argument outranks application.properties
    }

    // Fails on the baseline: see README.md, "Known baseline failures".
    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: the community dialect creates"
            + " IDENTITY ids as integer, and validation expects bigint for a Long id")
    @Test
    void createThenValidate() {
        try (var created = start("create")) {
            created.getBean(AuthorRepository.class).save(new Author("Schema", "schema@example.com"));
        }
        try (var validated = start("validate")) {
            assertThat(validated.getBean(AuthorRepository.class).findByEmail("schema@example.com")).isPresent();
        }
    }
}
