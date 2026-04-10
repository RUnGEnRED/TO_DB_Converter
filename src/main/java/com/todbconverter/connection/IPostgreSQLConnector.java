package com.todbconverter.connection;

import com.todbconverter.exception.ConnectionException;
import java.sql.Connection;

public interface IPostgreSQLConnector extends IDatabaseConnector {
    Connection getConnection() throws ConnectionException;
}
