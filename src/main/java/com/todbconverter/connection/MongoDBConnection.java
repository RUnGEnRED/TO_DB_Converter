package com.todbconverter.connection;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.todbconverter.exception.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDBConnection implements IDatabaseConnector, IMongoDBConnector {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBConnection.class);

    private final String connectionString;
    private final String databaseName;
    private MongoClient mongoClient;
    private MongoDatabase database;

    public MongoDBConnection(String host, int port, String databaseName, String username, String password) {
        if (username != null && !username.isEmpty() && password != null && !password.isEmpty()) {
            this.connectionString = String.format("mongodb://%s:%s@%s:%d", username, password, host, port);
        } else {
            this.connectionString = String.format("mongodb://%s:%d", host, port);
        }
        this.databaseName = databaseName;
    }

    public MongoDBConnection(String connectionString, String databaseName) {
        this.connectionString = connectionString;
        this.databaseName = databaseName;
    }

    public MongoDatabase getDatabase() throws ConnectionException {
        if (mongoClient == null) {
            connect();
        }
        return database;
    }

    @Override
    public void connect() throws ConnectionException {
        if (mongoClient == null) {
            try {
                logger.info("Connecting to MongoDB: {}", connectionString.replaceAll("://.*@", "://*****@"));
                mongoClient = MongoClients.create(connectionString);
                database = mongoClient.getDatabase(databaseName);
                logger.info("Successfully connected to MongoDB database: {}", databaseName);
            } catch (Exception e) {
                logger.error("Target database connection failed", e);
                throw new ConnectionException("Target database connection failed", e);
            }
        }
    }

    @Override
    public boolean isConnected() {
        try {
            return mongoClient != null && mongoClient.getDatabase(databaseName) != null;
        } catch (Exception e) {
            logger.error("Error checking MongoDB connection status", e);
            return false;
        }
    }

    @Override
    public void disconnect() {
        if (mongoClient != null) {
            mongoClient.close();
            logger.info("MongoDB connection closed");
        }
    }
}
