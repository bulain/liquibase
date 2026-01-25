package liquibase.database.core;

import liquibase.Scope;
import liquibase.database.DatabaseConnection;
import liquibase.database.OfflineConnection;
import liquibase.exception.DatabaseException;
import liquibase.executor.ExecutorService;
import liquibase.statement.core.RawParameterizedSqlStatement;

/**
 * GBase 8s
 */
public class Gbase8sDatabase extends InformixDatabase {

    private static final String PRODUCT_NAME = "GBase 8s Server";
    private static final String PRODUCT_NAME_DB2JCC_PREFIX = "IDS";

    public Gbase8sDatabase() {
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return 9088;
    }

    @Override
    public String getDefaultDriver(final String url) {
        if (url.startsWith("jdbc:gbasedbt-sqli")) {
            return "com.gbasedbt.jdbc.Driver";
        }
        return null;
    }

    @Override
    public String getShortName() {
        return "GBase8s";
    }

    @Override
    public boolean isCorrectDatabaseImplementation(final DatabaseConnection conn)
            throws DatabaseException {
        boolean correct = false;
        String name = conn.getDatabaseProductName();
        if (name != null && (name.equals(PRODUCT_NAME) || name.startsWith(PRODUCT_NAME_DB2JCC_PREFIX))) {
            correct = true;
        }
        return correct;
    }

    @Override
    public String getSystemSchema() {
        return "gbasedbt";
    }

    @Override
    protected String getConnectionSchemaName() {
        if ((getConnection() == null) || (getConnection() instanceof OfflineConnection)) {
            return null;
        }
        try {
            String schemaName = Scope.getCurrentScope().getSingleton(ExecutorService.class)
                    .getExecutor("jdbc", this)
                    .queryForObject(new RawParameterizedSqlStatement("select username from sysmaster.syssessions where sid = dbinfo('sessionid')"), String.class);
            if (schemaName != null) {
                return schemaName.trim();
            }
        } catch (Exception e) {
            Scope.getCurrentScope().getLog(getClass()).info("Error getting connection schema", e);
        }
        return null;
    }

}
