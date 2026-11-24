package io.github.randomcodespace.skylite.hibernate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToOne;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.model.relational.internal.SqlStringGenerationContextImpl;
import org.hibernate.mapping.Table;
import org.junit.jupiter.api.Test;

/** The schema the dialect generates, without a database. */
class SkyliteDialectTest {
    @Entity(name = "Parent")
    static class Parent {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        Long id;
    }

    @Entity(name = "Child")
    static class Child {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        Long id;

        @ManyToOne(optional = false)
        Parent parent;
    }

    @Entity(name = "Zebra")
    static class Zebra {
        @Id
        Long id;

        @ManyToOne
        Child child;
    }

    @Test
    void declaresForeignKeysInCreateTableAndDropsReferencingTablesFirst() {
        StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", SkyliteDialect.class.getName())
                .applySetting("hibernate.boot.allow_jdbc_metadata_access", "false")
                .build();
        try {
            Metadata metadata = new MetadataSources(registry)
                    .addAnnotatedClass(Parent.class).addAnnotatedClass(Child.class).addAnnotatedClass(Zebra.class)
                    .buildMetadata();
            var context = SqlStringGenerationContextImpl.forTests(metadata.getDatabase().getJdbcEnvironment());
            var exporter = new SkyliteDialect().getTableExporter();
            Map<String, Table> tables = new HashMap<>();
            metadata.collectTableMappings().forEach(t -> tables.put(t.getName(), t));

            // Zebra references Child, which references Parent: each is dropped before what it references.
            assertEquals(List.of("drop table if exists Zebra", "drop table if exists Child", "drop table if exists Parent"),
                    List.of(exporter.getSqlDropStrings(tables.get("Parent"), metadata, context)));
            String child = exporter.getSqlCreateStrings(tables.get("Child"), metadata, context)[0];
            assertTrue(child.matches(".*, constraint \\w+ foreign key \\(parent_id\\) references Parent \\(id\\)\\)"),
                    child);
            String zebra = exporter.getSqlCreateStrings(tables.get("Zebra"), metadata, context)[0];
            assertTrue(zebra.matches(".*foreign key \\(child_id\\) references Child \\(id\\)\\)"), zebra);
        } finally {
            StandardServiceRegistryBuilder.destroy(registry);
        }
    }

    @Test
    void generatedKeysComeFromTheDriver() {
        assertTrue(new SkyliteDialect().getDefaultUseGetGeneratedKeys());
    }

    @Test
    void batchesByDefault() {
        assertEquals(50, new SkyliteDialect().getDefaultStatementBatchSize());
    }
}
