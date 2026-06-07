package com.todbconverter.connection;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.todbconverter.config.DatabaseConfig;
import com.todbconverter.exception.ConnectionException;
import com.todbconverter.exception.TargetConnectionException;

import java.util.concurrent.TimeUnit;

/**
 * MongoDB connection using the official sync driver.
 */
public class MongoDBConnection implements IDatabaseConnector {

    private MongoClient client;
    private MongoDatabase database;
    private String databaseName;

    @Override
    public void connect(DatabaseConfig config) throws ConnectionException {
        String uri = config.getTargetMongoUri();
        databaseName = config.getTargetDatabase();

        if (uri == null || uri.isBlank()) {
            throw new TargetConnectionException("MongoDB URI is required");
        }

        if (databaseName == null || databaseName.isBlank()) {
            throw new TargetConnectionException("MongoDB database name is required");
        }

        try {
            ConnectionString connectionString = new ConnectionString(uri);

            MongoClientSettings settings = MongoClientSettings.builder()
                    .applyConnectionString(connectionString)
                    .applyToSocketSettings(builder ->
                            builder.connectTimeout(5, TimeUnit.SECONDS)
                                    .readTimeout(5, TimeUnit.SECONDS))
                    .applyToClusterSettings(builder ->
                            builder.serverSelectionTimeout(5, TimeUnit.SECONDS))
                    .build();

            this.client = MongoClients.create(settings);
            this.database = client.getDatabase(databaseName);
        } catch (Exception e) {
            throw new TargetConnectionException(
                    "Failed to connect to MongoDB: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean testConnection() throws ConnectionException {
        if (client == null || database == null) {
            throw new TargetConnectionException("Not connected. Call connect() first.");
        }

        try {
            // Simple command to test connection
            database.runCommand(new org.bson.Document("ping", 1));
            return true;
        } catch (Exception e) {
            throw new TargetConnectionException(
                    "MongoDB connection test failed: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isConnected() {
        return client != null && database != null;
    }

    /**
     * Get the MongoDB database instance.
     */
    public MongoDatabase getDatabase() {
        return database;
    }

    /**
     * Get a collection by name.
     */
    public com.mongodb.client.MongoCollection<org.bson.Document> getCollection(String collectionName) {
        if (database == null) {
            throw new IllegalStateException("Not connected");
        }
        return database.getCollection(collectionName);
    }

    @Override
    public void close() throws ConnectionException {
        if (client != null) {
            try {
                client.close();
            } catch (Exception e) {
                throw new TargetConnectionException(
                        "Error closing MongoDB connection: " + e.getMessage(), e);
            }
        }
    }
}
