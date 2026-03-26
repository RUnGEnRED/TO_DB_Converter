package com.todbconverter.connection;

/**
 * Interface for database connection management.
 * Provides methods to connect, disconnect, and check connection status.
 */
public interface IDatabaseConnector {
    
    /**
     * Establishes a connection to the database.
     * 
     * @throws Exception if connection fails
     */
    void connect() throws Exception;
    
    /**
     * Closes the database connection.
     */
    void disconnect();
    
    /**
     * Checks if the database connection is active.
     * 
     * @return true if connected, false otherwise
     */
    boolean isConnected();
}
