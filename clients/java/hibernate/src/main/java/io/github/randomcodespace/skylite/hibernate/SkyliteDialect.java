package io.github.randomcodespace.skylite.hibernate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.community.dialect.SQLiteDialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.mapping.Column;
import org.hibernate.mapping.ForeignKey;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.internal.StandardTableExporter;
import org.hibernate.tool.schema.spi.Exporter;

/**
 * Hibernate's community SQLite dialect for skylite's JDBC driver, with two differences:
 *
 * <ul>
 *   <li>Generated identifiers come from {@code getGeneratedKeys}, which the driver answers with the insert's rowid
 *       from the same request, never from a separate {@code select last_insert_rowid()}, which skylite would run on
 *       another SQLite connection.
 *   <li>Foreign keys are declared inside {@code create table}, since SQLite has no {@code alter table add
 *       constraint}, and skylite's connections enforce them. Dropping a table first drops the tables that reference
 *       it, so that the implicit delete of a drop never fails on their rows.
 * </ul>
 */
public class SkyliteDialect extends SQLiteDialect {
    private final Exporter<Table> tableExporter = new TableExporter(this);

    public SkyliteDialect() {}

    public SkyliteDialect(DialectResolutionInfo info) {
        super(info);
    }

    @Override
    public boolean getDefaultUseGetGeneratedKeys() {
        return true;
    }

    @Override
    public Exporter<Table> getTableExporter() {
        return tableExporter;
    }

    private static final class TableExporter extends StandardTableExporter {
        TableExporter(SkyliteDialect dialect) {
            super(dialect);
        }

        @Override
        protected void applyTableCheck(Table table, StringBuilder buf) {
            super.applyTableCheck(table, buf);
            for (ForeignKey fk : table.getForeignKeyCollection()) {
                if (!fk.isCreationEnabled() || !fk.isPhysicalConstraint()) {
                    continue;
                }
                Table target = fk.getReferencedTable();
                List<Column> to = fk.isReferenceToPrimaryKey() ? target.getPrimaryKey().getColumns()
                        : fk.getReferencedColumns();
                buf.append(", constraint ").append(fk.getName()).append(" foreign key (")
                        .append(names(fk.getColumns())).append(") references ")
                        .append(target.getQuotedName(dialect)).append(" (").append(names(to)).append(')');
            }
        }

        private String names(List<Column> columns) {
            StringBuilder sb = new StringBuilder();
            for (Column c : columns) {
                if (!sb.isEmpty()) {
                    sb.append(", ");
                }
                sb.append(c.getQuotedName(dialect));
            }
            return sb.toString();
        }

        @Override
        public String[] getSqlDropStrings(Table table, Metadata metadata, SqlStringGenerationContext context) {
            List<String> out = new ArrayList<>();
            drop(table, metadata, context, new HashSet<>(), out);
            return out.toArray(String[]::new);
        }

        /** Drops the tables that reference table, theirs first, then table itself. */
        private void drop(Table table, Metadata metadata, SqlStringGenerationContext context, Set<Table> seen,
                List<String> out) {
            if (!seen.add(table)) {
                return;
            }
            for (Table other : metadata.collectTableMappings()) {
                for (ForeignKey fk : other.getForeignKeyCollection()) {
                    if (other != table && fk.getReferencedTable() == table && fk.isCreationEnabled()
                            && fk.isPhysicalConstraint()) {
                        drop(other, metadata, context, seen, out);
                    }
                }
            }
            out.addAll(List.of(super.getSqlDropStrings(table, metadata, context)));
        }
    }
}
