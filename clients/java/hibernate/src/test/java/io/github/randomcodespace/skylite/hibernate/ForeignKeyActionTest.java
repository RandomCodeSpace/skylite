package io.github.randomcodespace.skylite.hibernate;

import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.persistence.Entity;
import jakarta.persistence.ForeignKey;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import java.util.HashMap;
import java.util.Map;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.model.relational.internal.SqlStringGenerationContextImpl;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.mapping.Table;
import org.junit.jupiter.api.Test;

/** Foreign key actions survive the dialect's table exporter. */
class ForeignKeyActionTest {
    @Entity(name = "CascadeParent")
    static class CascadeParent {
        @Id
        Long id;
    }

    @Entity(name = "CascadeChild")
    static class CascadeChild {
        @Id
        Long id;

        @ManyToOne
        @JoinColumn(name = "parent_id", foreignKey = @ForeignKey(name = "fk-parent"))
        @OnDelete(action = OnDeleteAction.CASCADE)
        CascadeParent parent;
    }

    @Test
    void onDeleteCascadeIsKeptAndAnUnusualConstraintNameIsQuoted() {
        StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", SkyliteDialect.class.getName())
                .applySetting("hibernate.boot.allow_jdbc_metadata_access", "false")
                .build();
        try {
            Metadata metadata = new MetadataSources(registry)
                    .addAnnotatedClass(CascadeParent.class)
                    .addAnnotatedClass(CascadeChild.class)
                    .buildMetadata();
            var context = SqlStringGenerationContextImpl.forTests(metadata.getDatabase().getJdbcEnvironment());
            var exporter = new SkyliteDialect().getTableExporter();
            Map<String, Table> tables = new HashMap<>();
            metadata.collectTableMappings().forEach(t -> tables.put(t.getName(), t));
            String ddl = exporter.getSqlCreateStrings(tables.get("CascadeChild"), metadata, context)[0];
            assertTrue(ddl.toLowerCase().contains("on delete cascade"), "DDL dropped ON DELETE CASCADE: " + ddl);
        } finally {
            StandardServiceRegistryBuilder.destroy(registry);
        }
    }
}
