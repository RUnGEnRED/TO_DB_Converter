package com.todbconverter.connection;

import com.mongodb.client.MongoDatabase;
import com.todbconverter.exception.ConnectionException;

public interface IMongoDBConnector extends IDatabaseConnector {
    MongoDatabase getDatabase() throws ConnectionException;
}