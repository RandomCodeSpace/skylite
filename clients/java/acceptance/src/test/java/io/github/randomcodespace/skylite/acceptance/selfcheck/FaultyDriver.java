package io.github.randomcodespace.skylite.acceptance.selfcheck;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.sql.rowset.serial.SerialBlob;

/**
 * A JDBC driver for URLs {@code jdbc:faulty:jdbc:sqlite:...} that delegates to sqlite-jdbc, used only to check that
 * the acceptance suite catches driver faults ({@link FaultCheck}).
 *
 * <p>It first repairs the five known baseline failures on the JDBC side, so that with no fault every test runs and
 * passes: it turns foreign keys on and declares the book table's foreign key, reports {@code id} columns as
 * BIGINT, implements {@code getBlob}, and raises constraint errors as SQL state 23000. Then it injects the one
 * fault named by {@code -Dacceptance.fault}, counts each time the fault changed a result, and prints the count on
 * exit, so a run shows that the fault was exercised.
 */
public final class FaultyDriver implements Driver {
    static final String PREFIX = "jdbc:faulty:";
    static final List<String> FAULTS = List.of(
            "commit-noop", "rollback-noop", "autocommit-ignored", "stale-read", "dirty-read",
            "update-count-one", "update-count-zero", "batch-count-one", "batch-drop-row", "batch-wrong-values",
            "batch-reordered", "generated-key-wrong", "generated-key-other-connection",
            "long-truncated", "double-rounded", "boolean-inverted", "decimal-scale-lost", "decimal-precision-lost",
            "instant-shifted", "date-off-by-one", "string-non-ascii-corrupted", "empty-string-as-null",
            "null-as-empty-string", "blob-altered", "blob-truncated", "blob-wrong-row",
            "params-swapped", "null-param-as-zero-or-empty",
            "limit-ignored", "offset-ignored", "order-by-ignored", "result-stops-early", "duplicate-rows",
            "label-last-column", "constraint-wrong-state", "error-swallowed",
            "product-name-wrong", "product-version-wrong");

    private static final String FAULT = System.getProperty("acceptance.fault", "none");
    private static final AtomicInteger injected = new AtomicInteger();
    private static final AtomicReference<Statement> lastInsert = new AtomicReference<>();
    private static final AtomicReference<byte[]> lastBytes = new AtomicReference<>();
    private static final Pattern LIMIT = Pattern.compile("(?i)\\blimit\\s+(\\?|\\d+)");
    private static final Pattern OFFSET = Pattern.compile("(?i)\\boffset\\s+(\\?|\\d+)");
    private static final Pattern ORDER_BY = Pattern.compile("(?i)\\s+order\\s+by\\s+.+?(?=\\s+limit\\b|\\s+offset\\b|\\)|$)");
    private static final long SHIFT_MILLIS = 2 * 3600_000L;

    static {
        if (!FAULT.equals("none") && !FAULTS.contains(FAULT)) {
            throw new IllegalArgumentException("unknown fault " + FAULT);
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() ->
                System.out.println("FAULT " + FAULT + " injected=" + injected.get())));
        try {
            DriverManager.registerDriver(new FaultyDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final Driver delegate;

    public FaultyDriver() {
        try {
            delegate = (Driver) Class.forName("org.sqlite.JDBC").getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("sqlite-jdbc is not on the classpath", e);
        }
    }

    private static boolean is(String fault) {
        return FAULT.equals(fault);
    }

    private static <T> T inject(T value) {
        injected.incrementAndGet();
        return value;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        String target = url.substring(PREFIX.length());
        if (is("dirty-read")) {
            // A shared cache with read_uncommitted lets one connection read another's uncommitted rows; sqlite-jdbc
            // shares the cache only for a file: URI.
            target = "jdbc:sqlite:file:" + target.substring("jdbc:sqlite:".length());
            target += (target.contains("?") ? "&" : "?") + "cache=shared&read_uncommitted=true";
            inject(null);
        }
        Connection real = delegate.connect(target, info);
        try (Statement s = real.createStatement()) {
            s.execute("pragma foreign_keys = on");
        }
        return (Connection) proxy(Connection.class, new ConnectionHandler(real, url));
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return delegate.getPropertyInfo(url.substring(PREFIX.length()), info);
    }

    @Override public int getMajorVersion() { return delegate.getMajorVersion(); }
    @Override public int getMinorVersion() { return delegate.getMinorVersion(); }
    @Override public boolean jdbcCompliant() { return false; }
    @Override public Logger getParentLogger() throws SQLFeatureNotSupportedException { return delegate.getParentLogger(); }

    private static Object proxy(Class<?> type, InvocationHandler handler) {
        return Proxy.newProxyInstance(FaultyDriver.class.getClassLoader(), new Class<?>[] {type}, handler);
    }

    /** Calls the real object, unwrapping reflection, and raises constraint errors (SQLite code 19) as SQL state 23000. */
    private static Object call(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof SQLException sql && (sql.getErrorCode() & 0xff) == 19) {
                if (is("constraint-wrong-state")) {
                    throw inject(new SQLException(sql.getMessage(), "42000", sql.getErrorCode(), sql));
                }
                throw new SQLIntegrityConstraintViolationException(sql.getMessage(), "23000", sql.getErrorCode(), sql);
            }
            throw e.getCause();
        }
    }

    private static String rewrite(String sql) {
        if (sql.startsWith("create table book ")) {
            return sql.substring(0, sql.lastIndexOf(')')) + ", foreign key (author_id) references author (id))";
        }
        if (!sql.stripLeading().toLowerCase().startsWith("select")) {
            return sql;
        }
        String result = sql;
        if (is("limit-ignored")) {
            result = LIMIT.matcher(result).replaceAll(m -> "limit (0 * " + Matcher.quoteReplacement(m.group(1)) + " - 1)");
        }
        if (is("offset-ignored")) {
            result = OFFSET.matcher(result).replaceAll(m -> "offset (0 * " + Matcher.quoteReplacement(m.group(1)) + ")");
        }
        if (is("order-by-ignored")) {
            result = ORDER_BY.matcher(result).replaceAll("");
        }
        return result.equals(sql) ? sql : inject(result);
    }

    private final class ConnectionHandler implements InvocationHandler {
        private final Connection real;
        private final String url;
        private final Map<String, Object> firstReads = new HashMap<>();
        private boolean requestedAutoCommit = true;

        ConnectionHandler(Connection real, String url) {
            this.real = real;
            this.url = url;
        }

        @Override
        public Object invoke(Object self, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "commit" -> {
                    if (is("commit-noop") || (is("autocommit-ignored") && !requestedAutoCommit)) {
                        return inject(null);
                    }
                }
                case "rollback" -> {
                    if ((is("rollback-noop") && args == null) || (is("autocommit-ignored") && !requestedAutoCommit)) {
                        return inject(null);
                    }
                }
                case "setAutoCommit" -> {
                    requestedAutoCommit = (Boolean) args[0];
                    if (is("autocommit-ignored")) {
                        return requestedAutoCommit ? null : inject(null);
                    }
                }
                case "getAutoCommit" -> {
                    if (is("autocommit-ignored")) {
                        return requestedAutoCommit;
                    }
                }
                case "getMetaData" -> {
                    return proxy(DatabaseMetaData.class, new MetaDataHandler((DatabaseMetaData) call(real, method, args), (Connection) self, url));
                }
                case "prepareStatement", "prepareCall" -> {
                    args[0] = rewrite((String) args[0]);
                    return statement((Statement) call(real, method, args), (Connection) self, (String) args[0]);
                }
                case "createStatement" -> {
                    return statement((Statement) call(real, method, args), (Connection) self, null);
                }
                default -> { }
            }
            return call(real, method, args);
        }

        private Object statement(Statement statement, Connection connection, String sql) {
            Class<?> type = statement instanceof PreparedStatement ? PreparedStatement.class : Statement.class;
            return proxy(type, new StatementHandler(statement, connection, this, sql));
        }

        boolean reallyAutoCommit() throws SQLException {
            return real.getAutoCommit();
        }
    }

    private static final class MetaDataHandler implements InvocationHandler {
        private final DatabaseMetaData real;
        private final Connection connection;
        private final String url;

        MetaDataHandler(DatabaseMetaData real, Connection connection, String url) {
            this.real = real;
            this.connection = connection;
            this.url = url;
        }

        @Override
        public Object invoke(Object self, Method method, Object[] args) throws Throwable {
            switch (method.getName()) {
                case "getURL" -> { return url; }
                case "getConnection" -> { return connection; }
                case "getDatabaseProductName" -> {
                    if (is("product-name-wrong")) {
                        return inject("PostgreSQL");
                    }
                }
                case "getDatabaseProductVersion" -> {
                    if (is("product-version-wrong")) {
                        return inject("3.8.0");
                    }
                }
                case "getDatabaseMinorVersion" -> {
                    if (is("product-version-wrong")) {
                        return inject(8);
                    }
                }
                case "getColumns" -> {
                    ResultSet columns = (ResultSet) call(real, method, args);
                    return proxy(ResultSet.class, (p, getter, getterArgs) -> {
                        Object value = call(columns, getter, getterArgs);
                        // Report an INTEGER id column as BIGINT, as the entities expect.
                        if (getterArgs != null && getterArgs.length == 1 && "id".equals(columns.getString("COLUMN_NAME"))
                                && columns.getInt("DATA_TYPE") == Types.INTEGER) {
                            if (getter.getName().equals("getInt") && "DATA_TYPE".equals(getterArgs[0])) {
                                return Types.BIGINT;
                            }
                            if (getter.getName().equals("getString") && "TYPE_NAME".equals(getterArgs[0])) {
                                return "bigint";
                            }
                        }
                        return value;
                    });
                }
                default -> { }
            }
            return call(real, method, args);
        }
    }

    private static final class StatementHandler implements InvocationHandler {
        private final Statement real;
        private final Connection connection;
        private final ConnectionHandler owner;
        private final String preparedSql;
        private final boolean batchFault = is("batch-drop-row") || is("batch-wrong-values") || is("batch-reordered");
        private final Map<Integer, Object[]> currentRow = new TreeMap<>();
        private final List<Map<Integer, Object[]>> batch = new ArrayList<>();
        private String lastSql;
        private int added;

        StatementHandler(Statement real, Connection connection, ConnectionHandler owner, String preparedSql) {
            this.real = real;
            this.connection = connection;
            this.owner = owner;
            this.preparedSql = preparedSql;
            this.lastSql = preparedSql;
        }

        @Override
        public Object invoke(Object self, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (name.equals("getConnection")) {
                return connection;
            }
            if (args != null && args.length > 0 && args[0] instanceof String sql
                    && (name.startsWith("execute") || name.equals("addBatch"))) {
                args[0] = rewrite(sql);
                lastSql = (String) args[0];
            }
            if (name.startsWith("set") && args != null && args.length >= 2 && args[0] instanceof Integer && preparedSql != null) {
                return bind(method, args);
            }
            if (name.equals("addBatch")) {
                added++;
            }
            if (batchFault && name.equals("addBatch") && args == null) {
                batch.add(new TreeMap<>(currentRow));
                return null;
            }
            if (name.equals("clearParameters")) {
                currentRow.clear();
            }
            if (batchFault && name.equals("executeBatch")) {
                return executeBatch(method);
            }
            if (name.equals("getGeneratedKeys") && is("generated-key-other-connection")) {
                Statement other = lastInsert.get();
                if (other != null && other != real) {
                    return results((ResultSet) call(inject(other), method, args), true);
                }
            }
            int pending = added;
            if (name.equals("executeBatch") || name.equals("clearBatch")) {
                added = 0;
            }
            Object result;
            try {
                result = call(real, method, args);
            } catch (SQLException e) {
                if (is("error-swallowed") && name.startsWith("execute") && !name.equals("executeQuery")) {
                    injected.incrementAndGet();
                    return switch (name) {
                        case "execute" -> false;
                        case "executeBatch" -> filled(new int[Math.max(1, pending)], 1);
                        case "executeLargeBatch" -> new long[] {1};
                        case "executeLargeUpdate" -> 1L;
                        default -> 1;
                    };
                }
                throw e;
            }
            if (name.startsWith("execute") && lastSql != null && lastSql.stripLeading().toLowerCase().startsWith("insert")) {
                lastInsert.set(real);
            }
            if ((name.equals("executeUpdate") || name.equals("getUpdateCount")) && result instanceof Integer count && count >= 0) {
                if (is("update-count-one") && count != 1) {
                    return inject(1);
                }
                if (is("update-count-zero") && count != 0) {
                    return inject(0);
                }
            }
            if (name.equals("executeLargeUpdate") && result instanceof Long count && count >= 0) {
                if (is("update-count-one") && count != 1) {
                    return inject(1L);
                }
                if (is("update-count-zero") && count != 0) {
                    return inject(0L);
                }
            }
            if (name.equals("executeBatch") && is("batch-count-one")) {
                int[] counts = (int[]) result;
                if (Arrays.stream(counts).anyMatch(c -> c != 1)) {
                    return inject(filled(counts, 1));
                }
            }
            if (result instanceof ResultSet rs) {
                boolean key = name.equals("getGeneratedKeys") || (lastSql != null && (lastSql.stripLeading().toLowerCase().startsWith("insert")
                        || lastSql.toLowerCase().contains("last_insert_rowid")));
                return results(rs, key);
            }
            return result;
        }

        private Object bind(Method method, Object[] args) throws Throwable {
            int index = (Integer) args[0];
            if (is("params-swapped") && (index == 1 || index == 2) && placeholders(preparedSql) >= 2) {
                args[0] = inject(3 - index);
            }
            if (is("null-param-as-zero-or-empty") && (method.getName().equals("setNull")
                    || (args[1] == null && method.getName().startsWith("setObject")))) {
                int type = method.getName().equals("setNull") ? (Integer) args[1]
                        : args.length > 2 && args[2] instanceof Integer t ? t : Types.INTEGER;
                injected.incrementAndGet();
                return switch (type) {
                    case Types.CHAR, Types.VARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR, Types.CLOB -> {
                        ((PreparedStatement) real).setString((Integer) args[0], "");
                        yield null;
                    }
                    case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> {
                        ((PreparedStatement) real).setBytes((Integer) args[0], new byte[0]);
                        yield null;
                    }
                    default -> {
                        ((PreparedStatement) real).setInt((Integer) args[0], 0);
                        yield null;
                    }
                };
            }
            currentRow.put((Integer) args[0], new Object[] {method, args.clone()});
            return call(real, method, args);
        }

        private Object executeBatch(Method method) throws Throwable {
            List<Map<Integer, Object[]>> rows = new ArrayList<>(batch);
            int sent = rows.size();
            batch.clear();
            if (is("batch-drop-row") && rows.size() > 1) {
                rows.removeFirst();
                injected.incrementAndGet();
            }
            if (is("batch-reordered") && rows.size() > 1) {
                Collections.reverse(rows);
                injected.incrementAndGet();
            }
            for (Map<Integer, Object[]> row : rows) {
                for (Object[] set : row.values()) {
                    Method setter = (Method) set[0];
                    Object[] setArgs = ((Object[]) set[1]).clone();
                    if (is("batch-wrong-values") && setter.getName().equals("setInt") && !Integer.valueOf(0).equals(setArgs[1])) {
                        setArgs[1] = inject(0);
                    }
                    call(real, setter, setArgs);
                }
                ((PreparedStatement) real).addBatch();
            }
            int[] counts = (int[]) call(real, method, null);
            // Report a count for every row sent, so that a dropped row is not visible in the counts.
            return counts.length == sent ? counts : filled(new int[sent], 1);
        }

        private Object results(ResultSet rs, boolean generatedKey) throws SQLException {
            String key = lastSql + " " + currentRow.values().stream().map(v -> Arrays.toString((Object[]) v[1])).toList();
            boolean stale = is("stale-read") && owner.reallyAutoCommit() && lastSql != null
                    && lastSql.stripLeading().toLowerCase().startsWith("select");
            return proxy(ResultSet.class, new ResultSetHandler(rs, generatedKey, stale ? owner.firstReads : null, key));
        }

        private static int placeholders(String sql) {
            return (int) sql.chars().filter(c -> c == '?').count();
        }
    }

    private static int[] filled(int[] counts, int value) {
        int[] result = counts.clone();
        Arrays.fill(result, value);
        return result;
    }

    private static final class ResultSetHandler implements InvocationHandler {
        private final ResultSet real;
        private final boolean generatedKey;
        private final Map<String, Object> firstReads;
        private final String key;
        private int row;
        private boolean duplicated;
        private Boolean wasNull;

        ResultSetHandler(ResultSet real, boolean generatedKey, Map<String, Object> firstReads, String key) {
            this.real = real;
            this.generatedKey = generatedKey;
            this.firstReads = firstReads;
            this.key = key;
        }

        @Override
        public Object invoke(Object self, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            switch (name) {
                case "next" -> {
                    if (is("duplicate-rows") && row == 1 && !duplicated) {
                        duplicated = true;
                        row++;
                        return inject(true);
                    }
                    if (is("result-stops-early") && row == 100) {
                        return real.next() ? inject(false) : false;
                    }
                    boolean more = real.next();
                    if (more) {
                        row++;
                    }
                    return more;
                }
                case "wasNull" -> {
                    return wasNull != null ? wasNull : real.wasNull();
                }
                case "findColumn" -> {
                    if (is("label-last-column")) {
                        int last = lastColumn((String) args[0]);
                        return last != real.findColumn((String) args[0]) ? inject(last) : last;
                    }
                }
                case "getBlob" -> {
                    byte[] bytes = bytes(args[0] instanceof Integer i ? real.getBytes(i) : real.getBytes((String) args[0]));
                    wasNull = bytes == null;
                    return bytes == null ? null : new SerialBlob(bytes);
                }
                default -> { }
            }
            if (!name.startsWith("get") || args == null || args.length == 0) {
                return call(real, method, args);
            }
            if (args[0] instanceof String label && is("label-last-column")) {
                int last = lastColumn(label);
                if (last != real.findColumn(label)) {
                    args[0] = inject(last);
                }
            }
            wasNull = null;
            Object value = call(real, method, args);
            if (firstReads != null && value != null) {
                Object first = firstReads.putIfAbsent(key + "|" + row + "|" + name + "|" + args[0], value);
                if (first != null && !first.equals(value) && !(first instanceof byte[])) {
                    return inject(first);
                }
            }
            return mangle(name, args, value);
        }

        private int lastColumn(String label) throws SQLException {
            var md = real.getMetaData();
            for (int i = md.getColumnCount(); i >= 1; i--) {
                if (md.getColumnLabel(i).equalsIgnoreCase(label)) {
                    return i;
                }
            }
            return real.findColumn(label);
        }

        private Object mangle(String name, Object[] args, Object value) throws SQLException {
            if (generatedKey && is("generated-key-wrong") && value instanceof Number n
                    && (name.equals("getLong") || name.equals("getInt") || name.equals("getObject"))) {
                injected.incrementAndGet();
                return switch (value) {
                    case Integer i -> i + 1000;
                    default -> n.longValue() + 1000;
                };
            }
            if (value == null) {
                if (is("null-as-empty-string") && name.equals("getString")) {
                    wasNull = false;
                    return inject("");
                }
                return null;
            }
            if (real.wasNull()) {
                return value;
            }
            switch (name) {
                case "getLong" -> {
                    long v = (Long) value;
                    if (is("long-truncated") && v != (int) v) {
                        return inject((long) (int) v);
                    }
                }
                case "getDouble" -> {
                    double v = (Double) value;
                    if (is("double-rounded") && v != (float) v) {
                        return inject((double) (float) v);
                    }
                }
                case "getBoolean" -> {
                    if (is("boolean-inverted")) {
                        return inject(!(Boolean) value);
                    }
                }
                case "getBigDecimal" -> {
                    BigDecimal v = (BigDecimal) value;
                    if (is("decimal-scale-lost") && v.scale() > 0) {
                        return inject(v.setScale(0, RoundingMode.HALF_UP));
                    }
                    if (is("decimal-precision-lost") && v.precision() > 6) {
                        return inject(v.round(new MathContext(6)));
                    }
                }
                case "getString" -> {
                    String v = (String) value;
                    if (is("string-non-ascii-corrupted") && v.chars().anyMatch(c -> c > 127)) {
                        return inject(v.replaceAll("[^\\x00-\\x7f]", "?"));
                    }
                    if (is("empty-string-as-null") && v.isEmpty()) {
                        wasNull = true;
                        return inject(null);
                    }
                }
                case "getBytes" -> {
                    return bytes((byte[]) value);
                }
                case "getTimestamp" -> {
                    if (is("instant-shifted")) {
                        Timestamp t = (Timestamp) value;
                        Timestamp shifted = new Timestamp(t.getTime() + SHIFT_MILLIS);
                        shifted.setNanos(t.getNanos());
                        return inject(shifted);
                    }
                }
                case "getDate" -> {
                    if (is("date-off-by-one")) {
                        return inject(Date.valueOf(((Date) value).toLocalDate().minusDays(1)));
                    }
                }
                case "getObject" -> {
                    if (is("instant-shifted")) {
                        switch (value) {
                            case OffsetDateTime t -> { return inject(t.plusNanos(SHIFT_MILLIS * 1_000_000)); }
                            case LocalDateTime t -> { return inject(t.plusNanos(SHIFT_MILLIS * 1_000_000)); }
                            case Timestamp t -> { return inject(new Timestamp(t.getTime() + SHIFT_MILLIS)); }
                            default -> { }
                        }
                    }
                    if (is("date-off-by-one")) {
                        switch (value) {
                            case LocalDate d -> { return inject(d.minusDays(1)); }
                            case Date d -> { return inject(Date.valueOf(d.toLocalDate().minusDays(1))); }
                            default -> { }
                        }
                    }
                }
                default -> { }
            }
            return value;
        }

        private static byte[] bytes(byte[] value) {
            if (value == null) {
                return null;
            }
            if (is("blob-altered") && value.length > 0) {
                byte[] altered = value.clone();
                altered[altered.length / 2] ^= 1;
                return inject(altered);
            }
            if (is("blob-truncated") && value.length > 0) {
                return inject(Arrays.copyOf(value, value.length - 1));
            }
            if (is("blob-wrong-row")) {
                byte[] previous = lastBytes.getAndSet(value);
                if (previous != null && !Arrays.equals(previous, value)) {
                    return inject(previous);
                }
            }
            return value;
        }
    }
}
