package com.todbconverter.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PostgreSQLConnection {
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

    public Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            logger.info("Connecting to PostgreSQL: {}", url);
            connection = DriverManager.getConnection(url, username, password);
            logger.info("Successfully connected to PostgreSQL");
        }
        return connection;
    }

    public void close() {
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
