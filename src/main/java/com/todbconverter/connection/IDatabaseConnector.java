package com.todbconverter.connection;

import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.exception.ConnectionException;

/**
 * Common interface for database connections.
 */
public interface IDatabaseConnector extends AutoCloseable {

    /**
     * Establish connection using the provided configuration.
     *
     * @param config database configuration
     * @throws ConnectionException if connection fails
     */
    void connect(DatabaseConfig config) throws ConnectionException;

    /**
     * Test if the connection is alive and working.
     *
     * @return true if connection is valid
     * @throws ConnectionException if connection test fails
     */
    boolean testConnection() throws ConnectionException;

    /**
     * Check if currently connected.
     */
    boolean isConnected();

    /**
     * Close the connection and release resources.
     */
    @Override
    void close() throws ConnectionException;
}
