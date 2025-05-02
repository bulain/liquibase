package liquibase.database.core;

import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

/**
 * Encapsulates OceanBase database support.
 */
public class OceanbaseDatabase extends MySQLDatabase {
    private static final String PRODUCT_NAME = "OceanBase";

    @Override
    public String getShortName() {
        return "oceanbase";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return 2881;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:oceanbase")) {
            return "com.oceanbase.jdbc.Driver";
        }
        return null;
    }

}
