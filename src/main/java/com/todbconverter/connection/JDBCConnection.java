package com.todbconverter.connection;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.exception.ConnectionException;
import com.todbconverter.exception.SourceConnectionException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * JDBC connection to any relational database.
 */
public class JDBCConnection implements IDatabaseConnector {

    private Connection connection;
    private String url;

    @Override
    public void connect(DatabaseConfig config) throws ConnectionException {
        String jdbcUrl = config.getSourceJdbcUrl();
        String username = config.getSourceUsername();
        String password = config.getSourcePassword();
        String driver = config.getSourceDriver();

        if (jdbcUrl == null || jdbcUrl.isBlank()) {
            throw new SourceConnectionException("JDBC URL is required");
        }

        try {
            // Load driver if specified
            if (driver != null && !driver.isBlank()) {
                Class.forName(driver);
            }

            this.url = jdbcUrl;
            this.connection = DriverManager.getConnection(jdbcUrl, username, password);
        } catch (ClassNotFoundException e) {
            throw new SourceConnectionException(
                    "JDBC driver not found: " + driver + ". Make sure the driver JAR is in classpath.", e);
        } catch (SQLException e) {
            throw new SourceConnectionException(
                    "Failed to connect to source database: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean testConnection() throws ConnectionException {
        if (connection == null) {
            throw new SourceConnectionException("Not connected. Call connect() first.");
        }

        try {
            return connection.isValid(5); // 5 second timeout
        } catch (SQLException e) {
            throw new SourceConnectionException("Connection test failed: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isConnected() {
        try {
            return connection != null && !connection.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Get the underlying JDBC connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Get database metadata.
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        if (connection == null) {
            throw new SQLException("Not connected");
        }
        return connection.getMetaData();
    }

    @Override
    public void close() throws ConnectionException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new SourceConnectionException("Error closing connection: " + e.getMessage(), e);
            }
        }
    }
}
