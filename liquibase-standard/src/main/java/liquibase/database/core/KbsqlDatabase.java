package liquibase.database.core;

import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

/**
 * Kingbase database support.
 */
public class KbsqlDatabase extends PostgresDatabase {

    public static final String PRODUCT_NAME = "KingbaseES";

    @Override
    public String getShortName() {
        return "kingbase";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return 54321;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:kingbase8:")) {
            return "com.kingbase8.Driver";
        }
        return null;
    }

}
