# TO DB Converter

> A bidirectional ETL tool for converting data between **PostgreSQL** (relational) and **MongoDB** (document) databases with automatic schema inference, relationship detection, and configurable MongoDB design patterns.


## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Key Features](#-key-features)
- [Technologies](#-technologies)
- [Getting Started](#-getting-started)
- [Usage](#-usage)
- [Relationship Detection](#-relationship-detection)
- [MongoDB Design Patterns](#-mongodb-design-patterns)
- [Testing](#-testing)


## 🌍 Overview

Converting between relational and document databases is notoriously difficult. Schema structures don't map cleanly — tables become collections, foreign keys become embedded arrays or references, and type systems diverge.

**TO DB Converter** automates this process by:

- **Discovering** the relational schema (tables, columns, PKs, FKs) and inferring relationship types (1:1, 1:N, M:N)
- **Transforming** data using configurable strategies — embed related documents or keep them as references
- **Applying** MongoDB design patterns (Attribute, Bucket, Subset, Outlier, Computed, Approximation) to optimize the target schema
- **Supporting** bidirectional conversion — PostgreSQL → MongoDB and MongoDB → PostgreSQL with automatic schema inference on the target side


## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                    ConverterService                            │
│                                                                │
│  ┌──────────────┐   ┌───────────────────┐   ┌───────────────┐  │
│  │  Extractor   │   │   Transformer     │   │    Loader     │  │
│  │              │   │                   │   │               │  │
│  │ PostgreSQL   │─▶│ UniversalTransf.   │─▶│ MongoDB       │  │
│  │ or MongoDB   │   │ + PatternOpt.     │   │ or PostgreSQL │  │
│  └──────────────┘   └───────────────────┘   └───────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

### Conversion Flow (PostgreSQL → MongoDB)

```
1. Connect to PostgreSQL
2. Extract schema metadata (tables, columns, PK, FK)
3. Detect relationship types (1:1, 1:N, M:N via junction tables)
4. Extract data in batches (1000 rows)
5. For each table:
   a. Build child indexes (parentId → [children])
   b. Build reference indexes (table → pk → record)
   c. Build M:N junction indexes
   d. Transform records: embed or reference based on strategy
   e. Apply MongoDB design patterns
   f. Export to MongoDB collection with indexes
6. Close connections
```

### Conversion Flow (MongoDB → PostgreSQL)

```
1. Connect to MongoDB
2. List collections (skip system.*)
3. Phase 1 — Schema Discovery: sample 10 docs per collection, infer tables/columns
4. Phase 2 — DDL Generation: DROP TABLE IF EXISTS, CREATE TABLE
5. Phase 3 — Data Loading: flatten documents to rows (batch insert)
6. Phase 4 — FK Restoration: ALTER TABLE ... ADD CONSTRAINT
7. Close connections
```


## ✨ Key Features

| Feature | Description |
|---|---|
| **Bidirectional ETL** | PostgreSQL ↔ MongoDB with automatic schema inference |
| **Relationship Detection** | Auto-detects 1:1, 1:N, M:N (junction tables), self-references |
| **Interactive Config Wizard** | TUI-based setup with JLine 3 — no manual config editing |
| **Per-Table Strategies** | EMBED or REFERENCE strategy configured per table |
| **M:N Mode Control** | FULL (embed docs) or IDS (store ID arrays) per relationship pair |
| **6 MongoDB Patterns** | Attribute, Bucket, Subset, Outlier, Computed, Approximation |
| **Batch Processing** | Efficient large-dataset handling with configurable batch sizes |
| **Native Type Handling** | JSONB → Map, TEXT[] → List, UUID, TIMESTAMP, DECIMAL |


## 🛠️ Technologies

| Layer | Technology |
|---|---|
| **Language** | Java 21 |
| **Build** | Maven 3.8+ |
| **Relational DB** | PostgreSQL 15 (JDBC 42.7.1) |
| **Document DB** | MongoDB 7 (Sync Driver 4.11.1) |
| **JSON** | Jackson 2.16.1 |
| **Logging** | SLF4J 2.0.9 |
| **TUI** | JLine 3.26.3 |
| **Testing** | JUnit 5.10.1 + Mockito 5.8.0 |


## 🚀 Getting Started

### Prerequisites

- [Java 21](https://adoptium.net/) or later
- [Maven 3.8+](https://maven.apache.org/)
- Docker + Docker Compose (for test environment)

### Build

```bash
mvn clean package
```

### Start Test Databases

```bash
docker compose -f docker-compose-test.yml up -d
```

### Stop

```bash
docker compose -f docker-compose-test.yml down -v
```


## 📖 Usage

### Interactive Wizard (Recommended)

```bash
java -jar target/to-db-converter-1.0-SNAPSHOT.jar --wizard
```

The wizard guides you through 4 steps:

| Step | Description | Controls |
|---|---|---|
| 1/4 | Database Connections | Type + Enter |
| 2/4 | Relationship Strategies | ↑↓ navigate, ←→ toggle, Tab for M:N |
| 3/4 | Schema Design Patterns | ↑↓ navigate, Space toggle |
| 4/4 | Conversion Direction | ↑↓ select |

### Headless Mode

Uses `application.properties` from the current working directory (falls back to classpath):

```bash
java -jar target/to-db-converter-1.0-SNAPSHOT.jar
```

### Docker Environments

| File | PG Port | Mongo Port | Database | User | Purpose |
|---|---|---|---|---|---|
| `docker-compose.yml` | 5433 | 27018 | `source_db` | `postgres` | Production |
| `docker-compose-test.yml` | 5432 | 27017 | `testdb` | `user` | Development |

Both load `database/init-postgres.sql` with 19 test tables covering 1:1, 1:N, M:N, self-ref, composite PK, JSON/JSONB, TEXT[], and reserved words.


## 🔍 Relationship Detection

The `MetadataExtractor` auto-detects relationship types from PostgreSQL schema metadata:

| Type | Detection Method |
|---|---|
| **1:1** | FK column is part of PK **or** has a UNIQUE index |
| **1:N** | FK column without uniqueness constraint |
| **M:N** | Junction table (exactly 2 FKs, ≤5 columns) **or** bidirectional FKs |
| **Self-ref** | FK pointing to the same table (e.g., `employees.manager_id`) |

### Junction Table Examples

| Table | FK1 | FK2 | Columns | Detected As |
|---|---|---|---|---|
| `enrollments` | `student_id` | `course_id` | 5 | M:N junction |
| `movie_roles` | `actor_id` | `movie_id` | 5 | M:N junction |
| `order_items` | `order_id` | `product_id` | 5 | M:N junction |
| `project_tasks` | `project_id` | `assignee_id` | 6 | 1:N (exceeds threshold) |

### Transformation Strategies

| Strategy | 1:1 | 1:N | M:N (FULL) | M:N (IDS) |
|---|---|---|---|---|
| **EMBED** | Embedded object | Embedded array | Embedded array | ID array (`*_ids`) |
| **REFERENCE** | `{table}_id` field | Separate collection | Embedded array | ID array (`*_ids`) |


## 🎨 MongoDB Design Patterns

Applied in order: **Computed → Approximation → Attribute → Bucket → Subset → Outlier**

### Attribute Pattern

Groups columns with common prefixes into an array:

```json
// Before: { release_US: "2024", release_FR: "2024", release_UK: "2024" }
// After:  { releases: [{k: "release_US", v: "2024", u: "US"}, ...] }
```

Threshold: minimum columns with same prefix (default: 3).

### Bucket Pattern

Chunks documents into bounded arrays by grouping key:

```json
{ "_id": "...", "bucket_id": 0, "count": 10, "data": [{...}, ...] }
```

Null keys are grouped into a separate `_NULL_` bucket.

### Subset Pattern

Splits large arrays into main document + `{table}_extras` collection. Items beyond the limit are moved to the extras collection.

### Outlier Pattern

Isolates oversized arrays into `{table}_outliers` collection. Main document gets `has_extras: true` flag.

### Computed Pattern

Pre-calculates derived fields from existing columns:

```properties
pattern.computed.fields=total:SUM(price,discount),count:COUNT(items)
```

### Approximation Pattern

Rounds numeric values to specified granularity:

```
40123 → 40100  (granularity=100)
```


## 🧪 Testing

### Run All Tests

```bash
docker compose -f docker-compose-test.yml up -d
mvn test
```

### Test Suite

| Test Class | Tests | Coverage |
|---|---|---|
| `DatabaseConfigTest` | 12 | Config loading, validation, strategies |
| `UniversalTransformerTest` | 16 | Bidirectional transformation, M:N, junction tables |
| `MongoDbPatternOptimizerTest` | 7 | All 6 MongoDB patterns |
| `MongoToSqlEdgeCaseTest` | 1 | Edge cases in Mongo→SQL flattening |
| `TypeMapperTest` | 2 | Type inference and mapping |
| `FullSchemaE2ETest` | 16 | Full 19-table schema, all patterns, all strategies |
| `ComprehensiveE2ETest` | 7 | EMBED, REFERENCE, IDS, patterns, round-trip |
| `MongoToPostgresE2ETest` | 4 | MongoDB→PostgreSQL conversion, FK restoration |
| `FullCycleE2ETest` | 1 | End-to-end round-trip verification |
| **Total** | **69** | |


## 📄 License

MIT License — see [LICENSE](LICENSE) for details.


<p align="center">
  <sub>Built with Java 21 • PostgreSQL • MongoDB • JLine 3</sub>
</p>
