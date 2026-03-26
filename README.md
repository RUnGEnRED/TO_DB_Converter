# DB Converter

A tool for converting data between relational databases (PostgreSQL) and document databases (MongoDB) with object nesting support.

## Project Description

A Java application that dynamically analyzes the structure of relational databases, retrieves data along with relationship metadata, and transforms it into nested JSON documents in MongoDB. The system supports **bidirectional conversion** between PostgreSQL and MongoDB.

## Features

- **PostgreSQL → MongoDB**: Convert relational tables to nested document structures
- **MongoDB → PostgreSQL**: Flatten document structures into relational tables
- **Metadata Extraction**: Read foreign keys, primary keys, and column information
- **Universal Mapping**: Transform data between relational and object-oriented models
- **Automatic Schema Inference**: Dynamically create table schemas from document structures

## Technologies

- **Language:** Java 21
- **Build Tool:** Maven
- **Databases:** 
  - PostgreSQL 15 (Relational Layer)
  - MongoDB 7 (Document Layer)
- **Libraries:**
  - PostgreSQL JDBC Driver
  - MongoDB Java Driver
  - Jackson (JSON mapping)
  - SLF4J (logging)

## Project Setup

### Prerequisites
- Docker Desktop installed and running
- Java JDK 17 or newer
- Maven 3.8+

### Step 1: Start Databases

```bash
docker-compose up -d
```

Check if containers are running:
```bash
docker ps
```

The following should be running:
- `to_db_postgres` (mapped to port 5433)
- `to_db_mongodb` (mapped to port 27018)

### Step 2: Build Project

```bash
mvn clean install
```

### Step 3: Run Application

```bash
mvn exec:java -Dexec.mainClass="com.todbconverter.Main"
```

Or after building the JAR:

```bash
java -jar target/to-db-converter-1.0-SNAPSHOT.jar
```

## Configuration

Edit `src/main/resources/application.properties` to configure conversion direction:

### PostgreSQL to MongoDB (default)
```properties
conversion.direction=POSTGRES_TO_MONGO
```

### MongoDB to PostgreSQL
```properties
conversion.direction=MONGO_TO_POSTGRES
```

## Architecture

The system follows an ETL (Extract, Transform, Load) pipeline:

1. **Extract**: Retrieve data and metadata from source database
2. **Transform**: Convert between relational and document models using foreign key relationships
3. **Load**: Insert transformed data into target database

### Key Components

- **MetadataExtractor**: Reads table schemas, columns, primary keys, and foreign keys from PostgreSQL
- **DataExtractor**: Retrieves flat relational data from PostgreSQL
- **MongoExtractor**: Extracts documents from MongoDB collections
- **UniversalTransformer**: Transforms data between relational and document formats
- **PostgresLoader**: Creates tables and loads data into PostgreSQL
- **MongoDBExporter**: Exports documents to MongoDB collections
