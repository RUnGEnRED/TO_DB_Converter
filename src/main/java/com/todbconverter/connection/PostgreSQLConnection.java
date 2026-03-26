package com.todbconverter.connection;

import com.todbconverter.exception.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreSQLConnection implements IDatabaseConnector {
    private static final Logger logger = LoggerFactory.getLogger(PostgreSQLConnection.class);

    private final String url;
    private final String username;
    private final String password;
    private Connection connection;

    public PostgreSQLConnection(String host, int port, String database, String username, String password) {
        this.url = String.format("jdbc:postgresql://%s:%d/%s", host, port, database);
        this.username = username;
        this.password = password;
    }

    public Connection getConnection() throws ConnectionException {
        try {
            if (connection == null || connection.isClosed()) {
                connect();
            }
        } catch (SQLException e) {
            logger.error("Error checking PostgreSQL connection status", e);
            throw new ConnectionException("Error checking PostgreSQL connection status", e);
        }
        return connection;
    }

    @Override
    public void connect() throws ConnectionException {
        try {
            if (connection == null || connection.isClosed()) {
                try {
                    logger.info("Connecting to PostgreSQL: {}", url);
                    connection = DriverManager.getConnection(url, username, password);
                    logger.info("Successfully connected to PostgreSQL");
                } catch (SQLException e) {
                    logger.error("Source database connection failed", e);
                    throw new ConnectionException("Source database connection failed", e);
                }
            }
        } catch (SQLException e) {
            logger.error("Error checking PostgreSQL connection status", e);
            throw new ConnectionException("Error checking PostgreSQL connection status", e);
        }
    }

    @Override
    public boolean isConnected() {
        try {
            return connection != null && !connection.isClosed();
        } catch (SQLException e) {
            logger.error("Error checking PostgreSQL connection status", e);
            return false;
        }
    }

    @Override
    public void disconnect() {
        if (connection != null) {
            try {
                connection.close();
                logger.info("PostgreSQL connection closed");
            } catch (SQLException e) {
                logger.error("Error closing PostgreSQL connection", e);
            }
        }
    }
}
