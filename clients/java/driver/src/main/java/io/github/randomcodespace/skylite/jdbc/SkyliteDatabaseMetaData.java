package io.github.randomcodespace.skylite.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;

/**
 * Minimal metadata: the product (SQLite and the server's {@code sqlite_version()}), the driver, and the constant
 * capabilities of this version, which reads only. Catalog queries such as getTables and getColumns are not
 * supported yet.
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
            try (Statement s = conn.createStatement(); ResultSet rs = s.executeQuery("SELECT sqlite_version()")) {
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
        return true;
    }

    // ---- capabilities of this version ----

    @Override public boolean supportsTransactions() { return false; }
    @Override public int getDefaultTransactionIsolation() { return Connection.TRANSACTION_SERIALIZABLE; }
    @Override public boolean supportsTransactionIsolationLevel(int level) { return level == Connection.TRANSACTION_SERIALIZABLE; }
    @Override public boolean supportsDataDefinitionAndDataManipulationTransactions() { return false; }
    @Override public boolean supportsDataManipulationTransactionsOnly() { return false; }
    @Override public boolean dataDefinitionCausesTransactionCommit() { return false; }
    @Override public boolean dataDefinitionIgnoredInTransactions() { return false; }
    @Override public boolean supportsMultipleTransactions() { return false; }
    @Override public boolean supportsSavepoints() { return false; }
    @Override public boolean supportsGetGeneratedKeys() { return false; }
    @Override public boolean supportsBatchUpdates() { return false; }
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

    // ---- catalog queries: not yet ----

    @Override public ResultSet getProcedures(String c, String s, String p) throws SQLException { throw catalog("getProcedures"); }
    @Override public ResultSet getProcedureColumns(String c, String s, String p, String col) throws SQLException { throw catalog("getProcedureColumns"); }
    @Override public ResultSet getTables(String c, String s, String t, String[] types) throws SQLException { throw catalog("getTables"); }
    @Override public ResultSet getSchemas() throws SQLException { throw catalog("getSchemas"); }
    @Override public ResultSet getSchemas(String c, String s) throws SQLException { throw catalog("getSchemas"); }
    @Override public ResultSet getCatalogs() throws SQLException { throw catalog("getCatalogs"); }
    @Override public ResultSet getTableTypes() throws SQLException { throw catalog("getTableTypes"); }
    @Override public ResultSet getColumns(String c, String s, String t, String col) throws SQLException { throw catalog("getColumns"); }
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
