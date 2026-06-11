# 🗄️ TO DB Converter

> A **PostgreSQL → MongoDB** ETL tool that extracts relational schema, detects relationships, transforms data through **5 MongoDB design patterns**, and loads it into document collections — all configurable via an interactive TUI wizard.

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Key Features](#-key-features)
- [Technologies](#-technologies)
- [Getting Started](#-getting-started)
- [Usage](#-usage)
- [Configuration](#-configuration)
- [Design Patterns](#-design-patterns)
- [Project Structure](#-project-structure)
- [Testing](#-testing)

---

## 🌍 Overview

**TO DB Converter** automates the migration of relational databases to MongoDB by applying schema analysis, foreign-key detection, and five of MongoDB's canonical design patterns during the transformation phase. The system uses a **Pipe-and-Filter** architecture with a topological sort that guarantees children are transformed before parents, enabling correct multi-level embedding.

Unlike naive migration tools that dump tables as-is, this converter intelligently restructures data: it embeds one-to-one and one-to-many relationships, limits oversized arrays, flags outliers, replaces wide columns with key-value arrays, pre-computes aggregations, and rounds numeric fields — all driven by a single `.properties` configuration file.

---

## 🏗️ Architecture

```
┌──────────┐     ┌──────────────┐    ┌──────────────┐     ┌──────────────┐
│  Config  │───▶│  JDBCSchema  │───▶│ JDBCData     │───▶│ Universal    │
│ .props / │     │  Extractor   │    │ Extractor    │     │ Transformer  │
│  Wizard  │     │ (schemas+FKs)│    │  (rows)      │     │ + 5 Patterns │
└──────────┘     └──────────────┘    └──────────────┘     └──────┬───────┘
                                                                  │
                                                                  ▼
                                                        ┌──────────────┐
                                                        │  MongoDB     │
                                                        │  Loader      │
                                                        └──────────────┘
```

### Pipeline Stages

| Stage | Component | Description |
|-------|-----------|-------------|
| **1. Configuration** | `DatabaseConfig` + `ConsoleWizard` | Load `.properties` or use TUI wizard to configure connections, strategies, and patterns |
| **2. Schema Extraction** | `JDBCSchemaExtractor` | JDBC `DatabaseMetaData` → tables, columns, PKs, FKs, cardinality (1:1 / 1:N / M:N) |
| **3. Data Extraction** | `JDBCDataExtractor` | `SELECT *` → `Map<String, List<Map<String, Object>>>` with type conversion |
| **4. Transformation** | `UniversalTransformer` + 5 `PatternApplier`s | Topological sort → relationship resolution (EMBED/REFERENCE) → pattern application |
| **5. Loading** | `MongoDBLoader` | `bulkWrite()` inserts, index creation, embed-skipping for CHILD_ENTITY tables |

---

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| **🔍 Automatic Schema Discovery** | Extracts tables, columns, PKs, FKs, cardinality (1:1/1:N/M:N) from any JDBC source |
| **🧠 Cycle & Self-Ref Detection** | Tarjan's DFS cycle detection + auto-downgrade to REFERENCE for cycles and self-references |
| **📦 5 MongoDB Design Patterns** | Attribute, Computed, Subset, Outlier, Approximation — applied in strict order |
| **🔗 Dual Relationship Strategy** | Per-edge EMBED/REFERENCE with safety fuse for unbounded arrays |
| **🛡️ 3-Layer EMBED+Outlier Validation** | Wizard warning / summary warning / pre-flight runtime crash |
| **🎛️ Interactive TUI Wizard** | JLine3-based 4-step wizard with matrix navigation and inline pattern config |
| **🔧 CLI Interface** | Picocli-powered `run`, `wizard`, `validate`, `report` commands |
| **🐳 Dockerized Test Environment** | PostgreSQL 16, MongoDB 7, pgAdmin — seed data auto-initialized |
| **✅ 139 Automated Tests** | Unit tests (JUnit 5 + AssertJ) + integration tests (H2 → MongoDB) |

---

## 🛠️ Technologies

| Layer | Technology |
|-------|------------|
| **Language** | Java 21 |
| **Build** | Maven 3.9+ (shaded JAR) |
| **CLI** | Picocli 4.7 |
| **TUI** | JLine 3 |
| **Source DB** | PostgreSQL 16 (via JDBC) — any JDBC-compatible works |
| **Target DB** | MongoDB 7 (Sync Driver 5.x) |
| **Test Framework** | JUnit 5 + AssertJ + Mockito |
| **In-Memory DB** | H2 (integration tests) |
| **Containers** | Docker + Docker Compose |
| **Logging** | SLF4J + Logback |

---

## 🚀 Getting Started

### Prerequisites

- Java 21 (LTS)
- Maven 3.9+
- Docker + Docker Compose (for test databases)

### Build & Run

```bash
# 1. Build shaded JAR
mvn clean package

# 2. Start databases (auto-seeded)
docker compose up -d

# 3a. Interactive configuration wizard
java -jar target/to-db-converter-1.0.0.jar wizard

# 3b. Run migration (after config is generated)
java -jar target/to-db-converter-1.0.0.jar run

# 3c. Validate connections only
java -jar target/to-db-converter-1.0.0.jar validate

# 3d. Generate HTML comparison report (after migration)
java -jar target/to-db-converter-1.0.0.jar report

# 4. Stop environment
docker compose down -v
```

---

## 📖 Usage

### CLI Commands

| Command | Aliases | Description |
|---------|---------|-------------|
| `run` | `-r`, `--run` | Full ETL: extract → transform → load |
| `wizard` | `-w`, `--wizard` | 4-step interactive TUI configuration |
| `validate` | `-v`, `--validate` | Test source + target connections |
| `report` | `-rp`, `--report` | Generate HTML report comparing source and target databases |

The `run`, `validate`, and `report` commands accept `--config=<path>` for a custom configuration file. The `report` command also accepts `--output=<path>` and `--samples=<N>`.

### Step-by-Step

#### 1. Start the Wizard

```bash
java -jar target/to-db-converter-1.0.0.jar wizard
```

The wizard walks through 4 steps:
- **Step 1** — Connection details (JDBC URL, credentials, MongoDB URI)
- **Step 2** — Relationship strategies (EMBED/REFERENCE per edge, shown as a table)
- **Step 3** — Design patterns (5-column matrix with inline config prompts)
- **Step 4** — Summary with warnings before saving

#### 2. Configure Connections

Set source PostgreSQL and target MongoDB connection parameters. The wizard tests connections before proceeding.

#### 3. Set Relationship Strategies

For each foreign-key edge, choose:
- **EMBED** — nest child documents inside the parent (default)
- **REFERENCE** — keep children in their own collection with FK

#### 4. Enable Design Patterns

Toggle patterns per table using the matrix. Each pattern requires a configuration string:

| Pattern | Format | Example |
|---------|--------|---------|
| Attribute | `arrayName=col:Key,...` | `personal=pesel:PESEL,birth_date:Data,bio:Bio` |
| Computed | `fieldName=FUNC(child.col)` | `employee_count=COUNT(employees.id)` |
| Subset | `childTable=limit` | `activity_logs=3` |
| Outlier | `childTable=threshold` | `activity_logs=3` |
| Approximation | `field1:gran1,field2:gran2` | `population:100,visits:1000` |

#### 5. Run Migration

```bash
java -jar target/to-db-converter-1.0.0.jar run
```

The system prints a live log of each stage and a summary on completion.

#### 6. Generate Comparison Report

After migration, generate an HTML report comparing source and target databases:

```bash
java -jar target/to-db-converter-1.0.0.jar report
```

The report (`report-<timestamp>.html`) includes:
- Source schema (tables, columns, types, PKs, FKs, sample data)
- Target collections (documents, sample JSON)
- Relationship mapping (child→parent, cardinality, EMBED/REFERENCE)
- Design pattern configuration per table

Use `--output` for a custom path and `--samples` to control the number of sample rows/documents.

---

## ⚙️ Configuration

The `db-converter.properties` file controls all aspects of the migration:

```properties
# Source database
source.jdbc.url=jdbc:postgresql://localhost:5433/source_db
source.jdbc.username=postgres
source.jdbc.password=password

# Target MongoDB
target.mongodb.uri=mongodb://admin:password@localhost:27018
target.mongodb.database=mydb_converted

# Default relationship strategy
relationship.strategy.default=EMBED

# Per-edge strategy overrides (child.parent=STRATEGY)
relationship.strategy.activity_logs.employees=REFERENCE
relationship.strategy.employee_details.employees=EMBED
relationship.strategy.employee_projects.employees=REFERENCE

# Design patterns
pattern.attribute.employee_details=personal=pesel:PESEL,birth_date:Data,bio:Bio
pattern.computed.departments=employee_count=COUNT(employees.id)
pattern.subset.employees=activity_logs=3
pattern.outlier.employees=activity_logs=3
pattern.approximation.metrics=population:100,visits:1000

# Safeguards
safeguard.max_children_per_parent=1000
```

### Strategy Resolution

Strategies are resolved with fallback: per-edge override → default. The `EdgeStrategyRegistry` checks both key directions (`child.parent` and `parent.child`) for user convenience.

---

## 🧩 Design Patterns

The system implements **5 MongoDB Design Patterns** applied in a fixed order:

```
Attribute → Computed → Subset → Outlier → Approximation
```

### 1. Attribute Pattern

Groups sparse columns into a `{key, value}` array — ideal for wide tables with many optional fields.

**Before:** `pesel: "90010112345"`, `birth_date: 1990-01-01`, `bio: "..."`  
**After:** `personal: [{key: "PESEL", value: "90010112345"}, {key: "Data", value: ISODate(...)}]`

### 2. Computed Pattern

Pre-computes aggregates (COUNT, SUM) during transformation — eliminates MongoDB `$lookup` overhead.

**Example:** `employee_count: 2` embedded in the department document.

### 3. Subset Pattern

Embeds only the N most recent child records under a `recent_<child>` field.

**Example:** `recent_activity_logs` with the latest 3 entries per employee.

### 4. Outlier Pattern

Caps embedded child arrays at a threshold and adds a `has_extras: true/false` flag. Requires **REFERENCE** strategy — remaining records live in the child collection.

### 5. Approximation Pattern

Rounds scalar numeric values to the nearest multiple of a granularity using `HALF_UP`.

**Example:** `population: 40123` (granularity 100) → `population: 40100`

### 3-Layer EMBED Validation

When Outlier conflicts with EMBED (Outlier requires REFERENCE), three layers catch it:

| Layer | Location | Action |
|-------|----------|--------|
| **1** | Wizard Step 3 | ⚠️ Yellow warning + pause |
| **2** | Wizard Step 4 (Summary) | 🔴 "Configuration Warnings" section |
| **3** | Pre-flight runtime | 💥 TransformationException, 0 collections written |

---

## 📁 Project Structure

```
TO_DB_Converter/
├── src/
│   ├── main/java/com/todbconverter/
│   │   ├── Main.java                    # Entry point + Picocli dispatch
│   │   ├── cli/commands/                # Run, Wizard, Validate, Report commands
│   │   ├── config/
│   │   │   ├── DatabaseConfig.java      # .properties load/save, strategies, patterns
│   │   │   └── EdgeStrategyRegistry.java # Per-edge EMBED/REFERENCE lookup
│   │   ├── core/
│   │   │   ├── extractor/
│   │   │   │   ├── JDBCSchemaExtractor.java  # Tables, FKs, cardinality
│   │   │   │   └── JDBCDataExtractor.java    # Row extraction + type conversion
│   │   │   ├── model/                   # TableMetadata, ColumnMetadata,
│   │   │   │                             # ForeignKeyMetadata, SchemaGraph, enums
│   │   │   ├── transformer/
│   │   │   │   ├── UniversalTransformer.java # Orchestrator: toposort, embed, patterns
│   │   │   │   └── patterns/
│   │   │   │       ├── PatternApplier.java       # Interface
│   │   │   │       ├── AttributePattern.java
│   │   │   │       ├── ComputedPattern.java
│   │   │   │       ├── SubsetPattern.java
│   │   │   │       ├── OutlierPattern.java
│   │   │   │       └── ApproximationPattern.java
│   │   │   └── loader/
│   │   │       └── MongoDBLoader.java    # bulkWrite, indexes, embed-skip
│   │   ├── report/
│   │   │   └── HtmlReportGenerator.java  # HTML report generation
│   │   ├── exception/                    # ConverterException hierarchy
│   │   ├── service/
│   │   │   └── ETLPipeline.java          # Orchestrator: extract → transform → load
│   │   └── ui/
│   │       ├── ConsoleWizard.java        # 4-step TUI wizard
│   │       └── TerminalRenderer.java     # ANSI formatting helpers
│   │
│   └── test/java/com/todbconverter/
│       ├── core/transformer/patterns/    # Pattern unit tests (28+14+...)
│       ├── core/transformer/             # UniversalTransformer, M:N tests
│       ├── integration/                  # H2 → MongoDB full E2E tests
│       └── ...
│
├── docker-compose.yml                    # PostgreSQL 16 + MongoDB 7 + pgAdmin
├── postgres-init/
│   ├── 01-schema.sql                     # Test schema (5+ tables)
│   └── 02-data.sql                       # Seed data
├── db-converter.properties               # Active configuration
├── docs/DOKUMENTACJA.md                  # Full technical documentation (Polish)
└── pom.xml
```

---

## 🧪 Testing

### Test Suite

| Category | Count |
|----------|-------|
| Unit tests | 136 |
| Integration tests | 3 |
| **Total** | **139** |

All pattern implementations have dedicated test classes covering edge cases (null safety, overflow, type coercion, threshold boundaries, malformed config).

### Run Tests

```bash
# Unit tests only
mvn test -Dtest='!*IntegrationTest'

# Full suite (requires Docker for integration tests)
mvn test

# Specific test class
mvn test -Dtest=UniversalTransformerTest
```

### Integration Test Environment

Integration tests use:
- **H2 in-memory** as the source database (no PostgreSQL dependency)
- **MongoDB at `localhost:27018`** (provided by `docker compose up -d`)

---

## 📄 License

This project is licensed under the terms of the MIT license.

---

<p align="center">
  <sub>Built with Java 21 • JDBC • MongoDB Sync Driver • JLine 3 • Picocli • Jackson</sub>
</p>
