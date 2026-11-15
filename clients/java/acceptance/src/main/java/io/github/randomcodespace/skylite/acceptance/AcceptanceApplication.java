package io.github.randomcodespace.skylite.acceptance;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** A minimal Spring Data JPA application; the tests drive it against a JDBC URL from configuration. */
@SpringBootApplication
public class AcceptanceApplication {
    public static void main(String[] args) {
        SpringApplication.run(AcceptanceApplication.class, args);
    }
}
