package liquibase.database.core;

import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;

/**
 * DM database support.
 */
public class DmsqlDatabase extends OracleDatabase {

    public static final String PRODUCT_NAME = "DM DBMS";

    @Override
    public String getShortName() {
        return "dm";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return PRODUCT_NAME;
    }

    @Override
    public Integer getDefaultPort() {
        return 5236;
    }

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return PRODUCT_NAME.equalsIgnoreCase(conn.getDatabaseProductName());
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jdbc:dm:")) {
            return "dm.jdbc.driver.DmDriver";
        }
        return null;
    }

    @Override
    public boolean supportsAutoIncrement() {
        return true;
    }

    @Override
    public int getIdentifierMaximumLength() {
        return LONG_IDENTIFIERS_LEGNTH;
    }

}
