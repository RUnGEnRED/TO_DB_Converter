# TO DB Converter

A bidirectional tool for converting data between PostgreSQL (relational) and MongoDB (document) databases with automatic schema inference and relationship handling.

## Features

- **PostgreSQL → MongoDB**: Convert relational tables to nested JSON documents with embedded relationships
- **MongoDB → PostgreSQL**: Flatten document structures into relational tables with foreign keys
- **Automatic Schema Inference**: Dynamically creates table schemas from document structures
- **Type Inference**: Automatically detects INTEGER, VARCHAR, TIMESTAMP types
- **Relationship Preservation**: Maintains foreign key relationships in both directions

## Quick Start

```bash
# Start databases
docker-compose up -d

# Build
mvn clean package

# Run POSTGRES → MONGO
java -jar target/to-db-converter-1.0-SNAPSHOT.jar -d POSTGRES_TO_MONGO

# Run MONGO → POSTGRES
java -jar target/to-db-converter-1.0-SNAPSHOT.jar -d MONGO_TO_POSTGRES
```

## Configuration

Edit `src/main/resources/application.properties`:

```properties
# Conversion direction: POSTGRES_TO_MONGO | MONGO_TO_POSTGRES
conversion.direction=POSTGRES_TO_MONGO

# PostgreSQL source
postgres.host=localhost
postgres.port=5433
postgres.database=source_db
postgres.username=${POSTGRES_USER:postgres}
postgres.password=${POSTGRES_PASSWORD:postgres}
postgres.schema=public

# MongoDB target
mongo.host=localhost
mongo.port=27018
mongo.database=target_db
mongo.username=${MONGO_USER:admin}
mongo.password=${MONGO_PASSWORD:admin}

# Drop existing tables before loading (MONGO_TO_POSTGRES only)
postgres.drop.existing=true
```

Or override direction via CLI:
```bash
java -jar target/to-db-converter-1.0-SNAPSHOT.jar -d POSTGRES_TO_MONGO
java -jar target/to-db-converter-1.0-SNAPSHOT.jar -d MONGO_TO_POSTGRES
```

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  PostgreSQL     │────▶│ Universal         │────▶│   MongoDB       │
│  (Source)       │     │ Transformer       │     │   (Target)      │
└─────────────────┘     └──────────────────┘     └─────────────────┘
        ▲                        │                        │
        │                        │                        │
        └──────────────┬─────────┴────────────────────────┘
                       │
        ┌──────────────▼──────────────────────────────┐
        │              ETL Pipeline                  │
        │  1. Extract (metadata + data)              │
        │  2. Transform (relational ↔ document)      │
        │  3. Load (insert into target)              │
        └────────────────────────────────────────────┘
```

### Key Components

| Component | Description |
|-----------|-------------|
| `MetadataExtractor` | Reads table schemas, columns, PK/FK from PostgreSQL |
| `DataExtractor` | Retrieves flat relational data |
| `MongoExtractor` | Extracts documents from MongoDB collections |
| `UniversalTransformer` | Bidirectional data transformation |
| `PostgresLoader` | Creates tables, loads data, adds FKs |
| `MongoDBExporter` | Exports documents to MongoDB collections |

## Tech Stack

- **Java 21** + Maven
- **PostgreSQL 15** (source)
- **MongoDB 7** (target)
- PostgreSQL JDBC Driver, MongoDB Java Driver, Jackson, SLF4J

## Development

```bash
# Run tests
mvn test

# Build JAR with dependencies
mvn package

# Clean build
mvn clean
```

## Docker Ports

| Service | Port |
|---------|------|
| PostgreSQL | 5433 |
| MongoDB | 27018 |
