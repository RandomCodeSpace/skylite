package io.github.randomcodespace.skylite.jdbc;

import io.github.randomcodespace.skylite.jdbc.FrameReader.Column;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Done;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Frame;
import io.github.randomcodespace.skylite.jdbc.FrameReader.Row;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Minimal metadata: the product (SQLite and the server's {@code sqlite_version()}), the driver, the constant
 * capabilities of this version, and getTables and getColumns, read from {@code sqlite_schema} and
 * {@code pragma_table_info}. Its queries are stateless reads, also inside a transaction. Other catalog queries are
 * not supported yet.
 */
final class SkyliteDatabaseMetaData implements DatabaseMetaData {
    private final SkyliteConnection conn;
    private volatile String productVersion;

    SkyliteDatabaseMetaData(SkyliteConnection conn) {
        this.conn = conn;
    }

    private static SQLException catalog(String what) {
        return Errors.unsupported("DatabaseMetaData." + what);
    }

    // ---- product and driver ----

    @Override
    public String getDatabaseProductName() {
        return "SQLite";
    }

    /** {@code SELECT sqlite_version()} on the server, once per metadata object. */
    @Override
    public String getDatabaseProductVersion() throws SQLException {
        if (productVersion == null) {
            try (PreparedStatement s = conn.metadataStatement("SELECT sqlite_version()"); ResultSet rs = s.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLNonTransientException("skylite: sqlite_version() returned no row", "HY000");
                }
                String v = rs.getString(1);
                if (v == null) {
                    throw new SQLNonTransientException("skylite: sqlite_version() returned NULL", "HY000");
                }
                productVersion = v;
            }
        }
        return productVersion;
    }

    private int productPart(int i) throws SQLException {
        String[] parts = getDatabaseProductVersion().split("\\.");
        try {
            return Integer.parseInt(parts[i]);
        } catch (RuntimeException e) {
            return 0;
        }
    }

    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        return productPart(0);
    }

    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        return productPart(1);
    }

    @Override
    public String getDriverName() {
        return "skylite JDBC driver";
    }

    @Override
    public String getDriverVersion() {
        return SkyliteDriver.VERSION;
    }

    @Override
    public int getDriverMajorVersion() {
        return SkyliteDriver.versionPart(0);
    }

    @Override
    public int getDriverMinorVersion() {
        return SkyliteDriver.versionPart(1);
    }

    @Override
    public int getJDBCMajorVersion() {
        return 4;
    }

    @Override
    public int getJDBCMinorVersion() {
        return 3;
    }

    @Override
    public String getURL() {
        return conn.url();
    }

    @Override
    public String getUserName() {
        return null;
    }

    @Override
    public Connection getConnection() {
        return conn;
    }

    @Override
    public boolean isReadOnly() {
        return conn.readOnly();
    }

    // ---- capabilities of this version ----

    @Override public boolean supportsTransactions() { return true; }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_SERIALIZABLE; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return level == Connection.TRANSACTION_SERIALIZABLE; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return true; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }
    @Override public boolean supportsMultipleTransactions() { return true; }
    @Override public boolean supportsSavepoints() { return true; }
    @Override public boolean supportsGetGeneratedKeys() { return true; }
    @Override public boolean supportsBatchUpdates() { return true; }
    @Override public boolean supportsNamedParameters() { return false; }
    @Override public boolean supportsMultipleOpenResults() { return false; }
    @Override public boolean supportsMultipleResultSets() { return false; }
    @Override public boolean supportsStatementPooling() { return false; }
    @Override public boolean supportsStoredProcedures() { return false; }
    @Override public boolean supportsStoredFunctionsUsingCallSyntax() { return false; }
    @Override public boolean autoCommitFailureClosesAllResultSets() { return false; }
    @Override public boolean generatedKeyAlwaysReturned() { return false; }
    @Override public boolean supportsResultSetType(int type) { return type == ResultSet.TYPE_FORWARD_ONLY; }
    @Override public boolean supportsResultSetConcurrency(int type, int concurrency) { return type == ResultSet.TYPE_FORWARD_ONLY && concurrency == ResultSet.CONCUR_READ_ONLY; }
    @Override public boolean supportsResultSetHoldability(int holdability) { return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public int getResultSetHoldability() { return ResultSet.CLOSE_CURSORS_AT_COMMIT; }
    @Override public boolean ownUpdatesAreVisible(int type) { return false; }
    @Override public boolean ownDeletesAreVisible(int type) { return false; }
    @Override public boolean ownInsertsAreVisible(int type) { return false; }
    @Override public boolean othersUpdatesAreVisible(int type) { return false; }
    @Override public boolean othersDeletesAreVisible(int type) { return false; }
    @Override public boolean othersInsertsAreVisible(int type) { return false; }
    @Override public boolean updatesAreDetected(int type) { return false; }
    @Override public boolean deletesAreDetected(int type) { return false; }
    @Override public boolean insertsAreDetected(int type) { return false; }
    @Override public boolean supportsPositionedDelete() { return false; }
    @Override public boolean supportsPositionedUpdate() { return false; }
    @Override public boolean supportsSelectForUpdate() { return false; }
    @Override public boolean supportsOpenCursorsAcrossCommit() { return false; }
    @Override public boolean supportsOpenCursorsAcrossRollback() { return false; }
    @Override public boolean supportsOpenStatementsAcrossCommit() { return true; }
    @Override public boolean supportsOpenStatementsAcrossRollback() { return true; }
    @Override public boolean locatorsUpdateCopy() { return true; }
    @Override public boolean supportsSharding() { return false; }
    @Override public RowIdLifetime getRowIdLifetime() { return RowIdLifetime.ROWID_UNSUPPORTED; }
    @Override public int getSQLStateType() { return sqlStateSQL; }

    // ---- SQL dialect: SQLite's ----

    @Override public boolean allProceduresAreCallable() { return false; }
    @Override public boolean allTablesAreSelectable() { return true; }
    @Override public boolean nullsAreSortedHigh() { return false; }
    @Override public boolean nullsAreSortedLow() { return true; }
    @Override public boolean nullsAreSortedAtStart() { return false; }
    @Override public boolean nullsAreSortedAtEnd() { return false; }
    @Override public boolean usesLocalFiles() { return false; }
    @Override public boolean usesLocalFilePerTable() { return false; }
    @Override public boolean supportsMixedCaseIdentifiers() { return false; }
    @Override public boolean storesUpperCaseIdentifiers() { return false; }
    @Override public boolean storesLowerCaseIdentifiers() { return false; }
    @Override public boolean storesMixedCaseIdentifiers() { return true; }
    @Override public boolean supportsMixedCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesUpperCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesLowerCaseQuotedIdentifiers() { return false; }
    @Override public boolean storesMixedCaseQuotedIdentifiers() { return true; }
    @Override public String getIdentifierQuoteString() { return "\""; }
    @Override public String getSQLKeywords() { return ""; }
    @Override public String getNumericFunctions() { return ""; }
    @Override public String getStringFunctions() { return ""; }
    @Override public String getSystemFunctions() { return ""; }
    @Override public String getTimeDateFunctions() { return ""; }
    @Override public String getSearchStringEscape() { return null; }
    @Override public String getExtraNameCharacters() { return ""; }
    @Override public boolean supportsAlterTableWithAddColumn() { return true; }
    @Override public boolean supportsAlterTableWithDropColumn() { return true; }
    @Override public boolean supportsColumnAliasing() { return true; }
    @Override public boolean nullPlusNonNullIsNull() { return true; }
    @Override public boolean supportsConvert() { return false; }
    @Override public boolean supportsConvert(int fromType, int toType) { return false; }
    @Override public boolean supportsTableCorrelationNames() { return true; }
    @Override public boolean supportsDifferentTableCorrelationNames() { return false; }
    @Override public boolean supportsExpressionsInOrderBy() { return true; }
    @Override public boolean supportsOrderByUnrelated() { return true; }
    @Override public boolean supportsGroupBy() { return true; }
    @Override public boolean supportsGroupByUnrelated() { return true; }
    @Override public boolean supportsGroupByBeyondSelect() { return true; }
    @Override public boolean supportsLikeEscapeClause() { return true; }
    @Override public boolean supportsNonNullableColumns() { return true; }
    @Override public boolean supportsMinimumSQLGrammar() { return true; }
    @Override public boolean supportsCoreSQLGrammar() { return false; }
    @Override public boolean supportsExtendedSQLGrammar() { return false; }
    @Override public boolean supportsANSI92EntryLevelSQL() { return false; }
    @Override public boolean supportsANSI92IntermediateSQL() { return false; }
    @Override public boolean supportsANSI92FullSQL() { return false; }
    @Override public boolean supportsIntegrityEnhancementFacility() { return false; }
    @Override public boolean supportsOuterJoins() { return true; }
    @Override public boolean supportsFullOuterJoins() { return true; }
    @Override public boolean supportsLimitedOuterJoins() { return true; }
    @Override public String getSchemaTerm() { return "schema"; }
    @Override public String getProcedureTerm() { return "procedure"; }
    @Override public String getCatalogTerm() { return "catalog"; }
    @Override public boolean isCatalogAtStart() { return true; }
    @Override public String getCatalogSeparator() { return "."; }
    @Override public boolean supportsSchemasInDataManipulation() { return false; }
    @Override public boolean supportsSchemasInProcedureCalls() { return false; }
    @Override public boolean supportsSchemasInTableDefinitions() { return false; }
    @Override public boolean supportsSchemasInIndexDefinitions() { return false; }
    @Override public boolean supportsSchemasInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsCatalogsInDataManipulation() { return false; }
    @Override public boolean supportsCatalogsInProcedureCalls() { return false; }
    @Override public boolean supportsCatalogsInTableDefinitions() { return false; }
    @Override public boolean supportsCatalogsInIndexDefinitions() { return false; }
    @Override public boolean supportsCatalogsInPrivilegeDefinitions() { return false; }
    @Override public boolean supportsSubqueriesInComparisons() { return true; }
    @Override public boolean supportsSubqueriesInExists() { return true; }
    @Override public boolean supportsSubqueriesInIns() { return true; }
    @Override public boolean supportsSubqueriesInQuantifieds() { return false; }
    @Override public boolean supportsCorrelatedSubqueries() { return true; }
    @Override public boolean supportsUnion() { return true; }
    @Override public boolean supportsUnionAll() { return true; }
    @Override public boolean doesMaxRowSizeIncludeBlobs() { return true; }
    @Override public boolean supportsRefCursors() { return false; }

    // ---- limits: 0 means none known ----

    @Override public int getMaxBinaryLiteralLength() { return 0; }
    @Override public int getMaxCharLiteralLength() { return 0; }
    @Override public int getMaxColumnNameLength() { return 0; }
    @Override public int getMaxColumnsInGroupBy() { return 0; }
    @Override public int getMaxColumnsInIndex() { return 0; }
    @Override public int getMaxColumnsInOrderBy() { return 0; }
    @Override public int getMaxColumnsInSelect() { return 0; }
    @Override public int getMaxColumnsInTable() { return 0; }
    @Override public int getMaxConnections() { return 0; }
    @Override public int getMaxCursorNameLength() { return 0; }
    @Override public int getMaxIndexLength() { return 0; }
    @Override public int getMaxSchemaNameLength() { return 0; }
    @Override public int getMaxProcedureNameLength() { return 0; }
    @Override public int getMaxCatalogNameLength() { return 0; }
    @Override public int getMaxRowSize() { return 0; }
    @Override public int getMaxStatementLength() { return 0; }
    @Override public int getMaxStatements() { return 0; }
    @Override public int getMaxTableNameLength() { return 0; }
    @Override public int getMaxTablesInSelect() { return 0; }
    @Override public int getMaxUserNameLength() { return 0; }

    // ---- catalog queries ----

    @Override public ResultSet getProcedures(String c, String s, String p) throws SQLException { throw catalog("getProcedures"); }
    @Override public ResultSet getProcedureColumns(String c, String s, String p, String col) throws SQLException { throw catalog("getProcedureColumns"); }
    @Override public ResultSet getSchemas() throws SQLException { throw catalog("getSchemas"); }
    @Override public ResultSet getSchemas(String c, String s) throws SQLException { throw catalog("getSchemas"); }
    @Override public ResultSet getCatalogs() throws SQLException { throw catalog("getCatalogs"); }
    @Override public ResultSet getTableTypes() throws SQLException { throw catalog("getTableTypes"); }
    @Override public ResultSet getColumnPrivileges(String c, String s, String t, String col) throws SQLException { throw catalog("getColumnPrivileges"); }
    @Override public ResultSet getTablePrivileges(String c, String s, String t) throws SQLException { throw catalog("getTablePrivileges"); }
    @Override public ResultSet getBestRowIdentifier(String c, String s, String t, int scope, boolean nullable) throws SQLException { throw catalog("getBestRowIdentifier"); }
    @Override public ResultSet getVersionColumns(String c, String s, String t) throws SQLException { throw catalog("getVersionColumns"); }
    @Override public ResultSet getPrimaryKeys(String c, String s, String t) throws SQLException { throw catalog("getPrimaryKeys"); }
    @Override public ResultSet getImportedKeys(String c, String s, String t) throws SQLException { throw catalog("getImportedKeys"); }
    @Override public ResultSet getExportedKeys(String c, String s, String t) throws SQLException { throw catalog("getExportedKeys"); }
    @Override public ResultSet getCrossReference(String pc, String ps, String pt, String fc, String fs, String ft) throws SQLException { throw catalog("getCrossReference"); }
    @Override public ResultSet getTypeInfo() throws SQLException { throw catalog("getTypeInfo"); }
    @Override public ResultSet getIndexInfo(String c, String s, String t, boolean unique, boolean approximate) throws SQLException { throw catalog("getIndexInfo"); }
    @Override public ResultSet getUDTs(String c, String s, String t, int[] types) throws SQLException { throw catalog("getUDTs"); }
    @Override public ResultSet getSuperTypes(String c, String s, String t) throws SQLException { throw catalog("getSuperTypes"); }
    @Override public ResultSet getSuperTables(String c, String s, String t) throws SQLException { throw catalog("getSuperTables"); }
    @Override public ResultSet getAttributes(String c, String s, String t, String a) throws SQLException { throw catalog("getAttributes"); }
    @Override public ResultSet getClientInfoProperties() throws SQLException { throw catalog("getClientInfoProperties"); }
    @Override public ResultSet getFunctions(String c, String s, String f) throws SQLException { throw catalog("getFunctions"); }
    @Override public ResultSet getFunctionColumns(String c, String s, String f, String col) throws SQLException { throw catalog("getFunctionColumns"); }
    @Override public ResultSet getPseudoColumns(String c, String s, String t, String col) throws SQLException { throw catalog("getPseudoColumns"); }

    /** Tables and views of the main database, without SQLite's own; catalog and schema are ignored. */
    @Override
    public ResultSet getTables(String catalog, String schema, String tablePattern, String[] types) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT NULL AS TABLE_CAT, NULL AS TABLE_SCHEM, name AS TABLE_NAME, "
                + "CASE type WHEN 'table' THEN 'TABLE' ELSE 'VIEW' END AS TABLE_TYPE, NULL AS REMARKS, "
                + "NULL AS TYPE_CAT, NULL AS TYPE_SCHEM, NULL AS TYPE_NAME, NULL AS SELF_REFERENCING_COL_NAME, "
                + "NULL AS REF_GENERATION FROM sqlite_schema WHERE type IN ('table', 'view') "
                + "AND name NOT LIKE 'sqlite\\_%' ESCAPE '\\' AND name LIKE ?");
        List<Object> params = new ArrayList<>(List.of(tablePattern == null ? "%" : tablePattern));
        if (types != null) {
            sql.append(" AND TABLE_TYPE IN (NULL");
            for (String t : types) {
                sql.append(", ?");
                params.add(t);
            }
            sql.append(')');
        }
        PreparedStatement ps = conn.metadataStatement(sql.append(" ORDER BY TABLE_TYPE, TABLE_NAME").toString());
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }
        ps.closeOnCompletion();
        try {
            return ps.executeQuery();
        } catch (SQLException e) {
            ps.close();
            throw e;
        }
    }

    private static final String[] COLUMNS = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE",
        "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS",
        "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE",
        "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT", "IS_GENERATEDCOLUMN"};
    private static final Pattern SIZE = Pattern.compile("\\(\\s*(\\d+)\\s*(?:,\\s*(\\d+)\\s*)?\\)");

    /**
     * The columns of tables and views of the main database. DATA_TYPE is the JDBC type of the declared type, as
     * {@link java.sql.ResultSetMetaData#getColumnType} reports it; TYPE_NAME is the declared type without its
     * arguments, which COLUMN_SIZE and DECIMAL_DIGITS hold. Catalog and schema are ignored.
     */
    @Override
    public ResultSet getColumns(String catalog, String schema, String tablePattern, String columnPattern)
            throws SQLException {
        List<Frame> rows = new ArrayList<>();
        try (PreparedStatement ps = conn.metadataStatement("SELECT m.name, p.name, p.type, p.\"notnull\", "
                + "p.dflt_value, p.cid + 1 FROM sqlite_schema AS m JOIN pragma_table_info(m.name) AS p "
                + "WHERE m.type IN ('table', 'view') AND m.name NOT LIKE 'sqlite\\_%' ESCAPE '\\' "
                + "AND m.name LIKE ? AND p.name LIKE ? ORDER BY m.name, p.cid")) {
            ps.setString(1, tablePattern == null ? "%" : tablePattern);
            ps.setString(2, columnPattern == null ? "%" : columnPattern);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String decl = rs.getString(3);
                    Matcher m = SIZE.matcher(decl == null ? "" : decl);
                    boolean sized = m.find();
                    boolean notNull = rs.getLong(4) != 0;
                    rows.add(new Row(new Object[] {null, null, rs.getString(1), rs.getString(2),
                        (long) SkyliteResultSetMetaData.jdbcType(decl == null ? "" : decl),
                        SkyliteResultSetMetaData.typeName(decl), sized ? Long.valueOf(m.group(1)) : null, null,
                        sized && m.group(2) != null ? Long.valueOf(m.group(2)) : null, 10L,
                        notNull ? (long) columnNoNulls : (long) columnNullable, null, rs.getString(5), null, null,
                        null, rs.getLong(6), notNull ? "NO" : "YES", null, null, null, null, "", ""}));
                }
            }
        }
        rows.add(new Done(0, true, 0, 0, 0, 0, null, null, 0, null, false));
        List<Column> cols = new ArrayList<>();
        for (String name : COLUMNS) {
            boolean number = name.equals("DATA_TYPE") || name.endsWith("_SIZE") || name.endsWith("_LENGTH")
                    || name.equals("DECIMAL_DIGITS") || name.equals("NUM_PREC_RADIX") || name.equals("NULLABLE")
                    || name.startsWith("SQL_") || name.equals("ORDINAL_POSITION") || name.equals("SOURCE_DATA_TYPE");
            cols.add(new Column(name, number ? "INTEGER" : "TEXT", null, null, null));
        }
        SkyliteStatement owner = (SkyliteStatement) conn.createStatement();
        owner.closeOnCompletion();
        return owner.publish(new SkyliteResultSet(owner, FrameReader.Frames.of(rows), cols, 0, 0));
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface != null && iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLNonTransientException("skylite: not a wrapper for " + (iface == null ? null : iface.getName()), "HY000");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return iface != null && iface.isInstance(this);
    }
}
